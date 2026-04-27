#ifndef LOG_HPP
#define LOG_HPP

#include <fcntl.h>
#include <unistd.h>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <stdexcept>
#include <string>
#include <fstream>
#include <vector>
#include "config.hpp"
#include "page.hpp"
#include "disk.hpp"

namespace sjtu {
#define LOGMANAGER_TYPE LogManager<KeyType, ValueType>
#define LOGMANAGER_TEMPLATE_ARGS template<typename KeyType, typename ValueType>

LOGMANAGER_TEMPLATE_ARGS
class LogManager {
private:
    int fd_;
    std::string file_name_;
    std::vector<uint8_t> buffer_;
    std::atomic<uint64_t> lsn_counter_;
    std::mutex log_mtx_;

    void append_bytes(const void* data, size_t size);

public:
    enum class LogType: uint8_t {
        PageUpdate = 0,
        RootUpdate = 1
    };

    typedef LogType log_type_t;

    struct LogEntry {
        uint64_t lsn_;
        log_type_t type_;
        diskpos_t pos_;
        size_t size_;
    };

    LogManager() = default;
    LogManager(const std::string& file_name);
    ~LogManager();

    LogManager(const LogManager& oth) = delete;
    LogManager& operator=(const LogManager& oth) = delete;

    void initialise(const std::string& file_name);

    void append_page_update(diskpos_t pos, const PAGE_TYPE& page);
    void append_root_update(diskpos_t root_pos);

    void flush();

    static bool needs_recovery(const std::string& file_name);
    static void recover(const std::string& file_name);

};

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_bytes(const void* data, size_t size) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    buffer_.insert(buffer_.end(), bytes, bytes + size);
}

LOGMANAGER_TEMPLATE_ARGS
LOGMANAGER_TYPE::LogManager(const std::string& file_name): file_name_(file_name), lsn_counter_(0) {
    fd_ = open(file_name.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open WAL file");
    }
}

LOGMANAGER_TEMPLATE_ARGS
LOGMANAGER_TYPE::~LogManager() {
    flush();
    if (fd_ != -1) {
        close(fd_);
    }
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::initialise(const std::string &file_name) {
    fd_ = open(file_name.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open WAL file");
    }
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_page_update(diskpos_t pos, const PAGE_TYPE& page) {
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::PageUpdate,
        .pos_ = pos,
        .size_ = sizeof(PAGE_TYPE)
    };

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&page, sizeof(PAGE_TYPE));
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_root_update(diskpos_t root_pos) {
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::RootUpdate,
        .pos_ = root_pos,
        .size_ = 0
    };

    append_bytes(&entry, sizeof(LogEntry));
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::flush() {
    std::lock_guard<std::mutex> lock(log_mtx_);
    if (!buffer_.empty()) {
        ssize_t written = write(fd_, reinterpret_cast<const char*>(buffer_.data()), buffer_.size());
        if (written != buffer_.size()) {
            throw std::runtime_error("WAL file write error");
        }
        if (fsync(fd_) == -1) {
            throw std::runtime_error("WAL file sync error");
        }
        buffer_.clear();
    }
}

LOGMANAGER_TEMPLATE_ARGS
bool LOGMANAGER_TYPE::needs_recovery(const std::string &file_name) {
    const std::string wal_file = file_name + ".wal";
    std::ifstream wal(wal_file, std::ios::binary | std::ios::ate);
    if (!wal.is_open()) return false;
    std::streampos size = wal.tellg();
    wal.close();
    return size > 0;
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::recover(const std::string& file_name) {
    const std::string wal_file = file_name + ".wal";

    std::ifstream wal(wal_file, std::ios::binary);
    if (!wal.is_open()) return;

    DiskManager<PAGE_TYPE, diskpos_t, 12, false> disk;
    disk.initialise(file_name);

    while (wal.good()) {
        LogEntry entry;
        wal.read(reinterpret_cast<char*>(&entry), sizeof(LogEntry));
        if (wal.eof()) break;
        if (wal.fail()) {
            throw std::runtime_error("WAL recovery: failed to read log entry header");
        }

        if (entry.type_ == LogType::PageUpdate) {
            PAGE_TYPE page;
            wal.read(reinterpret_cast<char*>(&page), sizeof(PAGE_TYPE));
            if (wal.fail()) {
                throw std::runtime_error("WAL recovery: failed to read page image");
            }
            disk.update(page, entry.pos_);
        }
        else if (entry.type_ == LogType::RootUpdate) {
            diskpos_t root = entry.pos_;
            disk.write_info(root, 2);
        }
        else {
            throw std::runtime_error("WAL recovery: unknown log type");
        }
    }

    wal.close();

    if (std::remove(wal_file.c_str()) != 0) {
        throw std::runtime_error("WAL recovery: failed to remove WAL file");
    }
}

} // namespace sjtu

#endif // LOG_HPP