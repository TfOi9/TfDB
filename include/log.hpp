#ifndef LOG_HPP
#define LOG_HPP

#include <fcntl.h>
#include <unistd.h>
#include <cstddef>
#include <cstdint>
#include <cstdio>
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
    int fd_ = -1;
    std::string file_name_;
    std::vector<uint8_t> buffer_;
    std::atomic<uint64_t> lsn_counter_{0};
    std::mutex log_mtx_;

    void append_bytes(const void* data, size_t size);

public:
    enum class LogType: uint8_t {
        PageInit = 0,
        SlotInsert,
        SlotDelete,
        SlotUpdate,
        SlotRangeInit,
        MetaUpdate,
        RootUpdate
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
    void append_slot_insert(diskpos_t pos, int slot_idx, diskpos_t child_pos, const KEYPAIR_TYPE& kp);
    void append_slot_delete(diskpos_t pos, int slot_idx);
    void append_slot_update(diskpos_t pos, int slot_idx, const KEYPAIR_TYPE& kp);
    void append_slot_range_init(diskpos_t pos, int slot_start, int slot_count, bool is_leaf, const PAGE_TYPE& page);
    void append_meta_update(diskpos_t pos, uint64_t mask, const PAGE_TYPE& page);

    void flush();

    static bool needs_recovery(const std::string& file_name);
    static void recover(const std::string& file_name);

};

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_bytes(const void* data, size_t size) {
    if (!USE_WAL) return;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    buffer_.insert(buffer_.end(), bytes, bytes + size);
}

LOGMANAGER_TEMPLATE_ARGS
LOGMANAGER_TYPE::LogManager(const std::string& file_name) {
    if (!USE_WAL) return;
    initialise(file_name);
}

LOGMANAGER_TEMPLATE_ARGS
LOGMANAGER_TYPE::~LogManager() {
    if (!USE_WAL) return;
    flush();
    if (fd_ != -1) {
        close(fd_);
    }
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::initialise(const std::string &file_name) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }

    file_name_ = file_name + ".wal";
    buffer_.clear();
    lsn_counter_.store(0, std::memory_order_relaxed);

    fd_ = open(file_name_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open WAL file");
    }
}

/* Simply throw all the page data into WAL. Very expensive. Use only when you have to. */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_page_update(diskpos_t pos, const PAGE_TYPE& page) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::PageInit,
        .pos_ = pos,
        .size_ = sizeof(PAGE_TYPE)
    };

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&page, sizeof(PAGE_TYPE));
}

/* Update the root position of BPT. Cheap. */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_root_update(diskpos_t root_pos) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::RootUpdate,
        .pos_ = root_pos,
        .size_ = 0
    };

    append_bytes(&entry, sizeof(LogEntry));
}

/* Insert one slot into one page. Relatively cheap. */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_insert(diskpos_t pos, int slot_idx, diskpos_t child_pos, const KEYPAIR_TYPE& kp) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotInsert,
        .pos_ = pos,
        .size_ = sizeof(int) + sizeof(diskpos_t) + sizeof(KEYPAIR_TYPE)
    };

    append_bytes(&slot_idx, sizeof(int));
    append_bytes(&child_pos, sizeof(diskpos_t));
    append_bytes(&kp, sizeof(KEYPAIR_TYPE));
}

/* Delete one slot in one page. Cheap. */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_delete(diskpos_t pos, int slot_idx) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotDelete,
        .pos_ = pos,
        .size_ = sizeof(int)
    };

    append_bytes(&slot_idx, sizeof(int));
}

/* Update one slot in one page. Relatively Cheap. */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_update(diskpos_t pos, int slot_idx, const KEYPAIR_TYPE& kp) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotUpdate,
        .pos_ = pos,
        .size_ = sizeof(int) + sizeof(KEYPAIR_TYPE)
    };

    append_bytes(&slot_idx, sizeof(int));
    append_bytes(&kp, sizeof(KEYPAIR_TYPE));
}

/* Update a range of slots in one page. Expensive. (Depends on slot_count.) */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_range_init(diskpos_t pos, int slot_start, int slot_count, bool is_leaf, const PAGE_TYPE& page) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotRangeInit,
        .pos_ = pos,
        .size_ = sizeof(int) * 2 + sizeof(bool) + sizeof(KEYPAIR_TYPE) * slot_count + sizeof(diskpos_t) * (is_leaf ? 0 : slot_count)
    };

    append_bytes(&slot_start, sizeof(int));
    append_bytes(&slot_count, sizeof(int));
    append_bytes(&is_leaf, sizeof(bool));
    for (int i = 0; i < slot_count; i++) {
        append_bytes(&page.data_[i + slot_start], sizeof(KEYPAIR_TYPE));
    }
    if (is_leaf) return;
    for (int i = 0; i < slot_count; i++) {
        append_bytes(&page.ch_[i + slot_start], sizeof(diskpos_t));
    }
}

/* Update metadata of one page. Cheap. */
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_meta_update(diskpos_t pos, uint64_t mask, const PAGE_TYPE& page) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotRangeInit,
        .pos_ = pos,
        .size_ = 8 + 8 * __builtin_popcount(mask)
    };

    append_bytes(&mask, 8);
    if (mask & (1 << 4)) {
        // Type update
        uint64_t type = static_cast<uint64_t>(&page.type_);
        append_bytes(&type, 8);
    }
    if (mask & (1 << 3)) {
        // Parent update
        append_bytes(&page.fa_, 8);
    }
    if (mask & (1 << 2)) {
        // Left update
        append_bytes(&page.left_, 8);
    }
    if (mask & (1 << 1)) {
        // Right update
        append_bytes(&page.right_, 8);
    }
    if (mask & 1) {
        // Size update
        append_bytes(&page.size_, 8);
    }
}
 
LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::flush() {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);
    if (!buffer_.empty()) {
        if (fd_ == -1) {
            throw std::runtime_error("WAL file is not initialized");
        }

        size_t offset = 0;
        while (offset < buffer_.size()) {
            ssize_t written = write(fd_, reinterpret_cast<const char*>(buffer_.data() + offset), buffer_.size() - offset);
            if (written <= 0) {
                throw std::runtime_error("WAL file write error");
            }
            offset += static_cast<size_t>(written);
        }
        if (fsync(fd_) == -1) {
            throw std::runtime_error("WAL file sync error");
        }
        buffer_.clear();
    }
}

LOGMANAGER_TEMPLATE_ARGS
bool LOGMANAGER_TYPE::needs_recovery(const std::string &file_name) {
    if (!USE_WAL) return false;
    const std::string wal_file = file_name + ".wal";
    std::ifstream wal(wal_file, std::ios::binary | std::ios::ate);
    if (!wal.is_open()) return false;
    std::streampos size = wal.tellg();
    wal.close();
    return size > 0;
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::recover(const std::string& file_name) {
    if (!USE_WAL) return;
    const std::string wal_file = file_name + ".wal";

    std::ifstream wal(wal_file, std::ios::binary);
    if (!wal.is_open()) return;

    DiskManager<PAGE_TYPE, diskpos_t, 12, false> disk;
    disk.initialise(file_name);

    while (wal.good()) {
        LogEntry entry;
        wal.read(reinterpret_cast<char*>(&entry), sizeof(LogEntry));
        std::streamsize header_bytes = wal.gcount();
        if (header_bytes == 0) {
            break;
        }
        if (header_bytes != sizeof(LogEntry)) {
            break;
        }

        bool bad = false;
        switch (entry.type_) {
            case LogType::PageInit: {
                if (entry.size_ != sizeof(PAGE_TYPE)) {
                    throw std::runtime_error("WAL recovery: invalid page payload size");
                }
                PAGE_TYPE page;
                wal.read(reinterpret_cast<char*>(&page), sizeof(PAGE_TYPE));
                if (wal.gcount() != sizeof(PAGE_TYPE)) {
                    bad = true;
                    break;
                }
                disk.update(page, entry.pos_);
                break;
            }

            case LogType::RootUpdate: {
                if (entry.size_ != 0) {
                    throw std::runtime_error("WAL recovery: invalid root payload size");
                }
                diskpos_t root = entry.pos_;
                disk.write_info(root, 2);
                break;
            }

            case LogType::SlotInsert: {
                if (entry.size_ != sizeof(int) + sizeof(diskpos_t) + sizeof(KEYPAIR_TYPE)) {
                    throw std::runtime_error("WAL recovery: invalid page payload size");
                }
                int slot_idx;
                diskpos_t child_pos;
                KEYPAIR_TYPE kp;
                wal.read(reinterpret_cast<char*>(&slot_idx), sizeof(int));
                if (wal.gcount() != sizeof(int)) {
                    bad = true;
                    break;
                }
                wal.read(reinterpret_cast<char*>(&child_pos), sizeof(diskpos_t));
                if (wal.gcount() != sizeof(diskpos_t)) {
                    bad = true;
                    break;
                }
                wal.read(reinterpret_cast<char*>(&kp), sizeof(KEYPAIR_TYPE));
                if (wal.gcount() != sizeof(KEYPAIR_TYPE)) {
                    bad = true;
                    break;
                }
                PAGE_TYPE page;
                disk.read(page, entry.pos_);
                for (int i = page.size_ - 1; i >= slot_idx; i--) {
                    page.data_[i + 1] = page.data_[i];
                    page.ch_[i + 1] = page.ch_[i];
                }
                page.data_[slot_idx] = kp;
                page.ch_[slot_idx] = child_pos;
                page.size_++;
                disk.update(page, entry.pos_);
                break;
            }

            case LogType::SlotDelete: {
                if (entry.size_ != sizeof(int)) {
                    throw std::runtime_error("WAL recovery: invalid page payload size");
                }
                int slot_idx;
                wal.read(reinterpret_cast<char*>(slot_idx), sizeof(int));
                if (wal.gcount() != sizeof(int)) {
                    bad = true;
                    break;
                }
                PAGE_TYPE page;
                disk.read(page, entry.pos_);
                for (int i = slot_idx; i < page.size_ - 1; i++) {
                    page.data_[i] = page.data_[i + 1];
                    page.ch_[i] = page.ch_[i + 1];
                }
                page.size_--;
                disk.update(page, entry.pos_);
                break;
            }

            case LogType::SlotUpdate: {
                if (entry.size_ != sizeof(int) + sizeof(KEYPAIR_TYPE)) {
                    throw std::runtime_error("WAL recovery: invalid page payload size");
                }
                int slot_idx;
                KEYPAIR_TYPE kp;
                wal.read(reinterpret_cast<char*>(&slot_idx), sizeof(int));
                if (wal.gcount() != sizeof(int)) {
                    bad = true;
                    break;
                }
                wal.read(reinterpret_cast<char*>(&kp), sizeof(KEYPAIR_TYPE));
                if (wal.gcount() != sizeof(KEYPAIR_TYPE)) {
                    bad = true;
                    break;
                }
                PAGE_TYPE page;
                disk.read(page, entry.pos_);
                page.data_[slot_idx] = kp;
                disk.update(page, entry.pos_);
                break;
            }

            case LogType::SlotRangeInit: {
                int slot_start, slot_count;
                bool is_leaf;
                wal.read(reinterpret_cast<char*>(slot_start), sizeof(int));
                if (wal.gcount() != sizeof(int)) {
                    bad = true;
                    break;
                }
                wal.read(reinterpret_cast<char*>(slot_count), sizeof(int));
                if (wal.gcount() != sizeof(int)) {
                    bad = true;
                    break;
                }
                wal.read(reinterpret_cast<char*>(is_leaf), sizeof(bool));
                if (wal.gcount() != sizeof(bool)) {
                    bad = true;
                    break;
                }
                int expected_size = sizeof(int) * 2 + sizeof(bool) + sizeof(KEYPAIR_TYPE) * slot_count + sizeof(diskpos_t) * (is_leaf ? 0 : slot_count);
                if (expected_size != entry.size_) {
                    throw std::runtime_error("WAL recovery: invalid page payload size");
                }
                PAGE_TYPE new_page;
                for (int i = 0; i < slot_count; i++) {
                    wal.read(reinterpret_cast<char*>(&new_page.data_[i + slot_start]), sizeof(KEYPAIR_TYPE));
                    if (wal.gcount() != sizeof(KEYPAIR_TYPE)) {
                        bad = true;
                    break;
                    }
                }
                if (!is_leaf) {
                    for (int i = 0; i < slot_count; i++) {
                        wal.read(reinterpret_cast<char*>(&new_page.ch_[i + slot_start]), sizeof(diskpos_t));
                        if (wal.gcount() != sizeof(diskpos_t)) {
                            bad = true;
                    break;
                        }
                    }
                }
                PAGE_TYPE page;
                disk.read(page, entry.pos_);
                for (int i = 0; i < slot_count; i++) {
                    page.data_[i + slot_start] = new_page.data_[i + slot_start];
                    if (!is_leaf) {
                        page.ch_[i + slot_start] = new_page.ch_[i + slot_start];
                    }
                }
                disk.update(page, entry.pos_);
                break;
            }

            case LogType::MetaUpdate: {
                int mask, temp;
                wal.read(reinterpret_cast<char*>(&mask), 8);
                if (wal.gcount() != 8) {
                    bad = true;
                    break;
                }
                if (entry.size_ != 8 + 8 * __builtin_popcount(mask)) {
                    throw std::runtime_error("WAL recovery: invalid page payload size");
                }
                PAGE_TYPE page;
                disk.read(page, entry.pos_);
                if (mask & (1 << 4)) {
                    wal.read(reinterpret_cast<char*>(&temp), 8);
                    if (wal.gcount() != 8) {
                        bad = true;
                    break;
                    }
                    if (temp == 1) {
                        page.type_ = PageType::Leaf;
                    }
                    else if (temp == 2) {
                        page.type_ = PageType::Internal;
                    }
                }
                if (mask & (1 << 3)) {
                    wal.read(reinterpret_cast<char*>(&temp), 8);
                    if (wal.gcount() != 8) {
                        bad = true;
                    break;
                    }
                    page.fa_ = temp;
                }
                if (mask & (1 << 2)) {
                    wal.read(reinterpret_cast<char*>(&temp), 8);
                    if (wal.gcount() != 8) {
                        bad = true;
                    break;
                    }
                    page.left_ = temp;
                }
                if (mask & (1 << 1)) {
                    wal.read(reinterpret_cast<char*>(&temp), 8);
                    if (wal.gcount() != 8) {
                        bad = true;
                    break;
                    }
                    page.right_ = temp;
                }
                if (mask & 1) {
                    wal.read(reinterpret_cast<char*>(&temp), 8);
                    if (wal.gcount() != 8) {
                        bad = true;
                    break;
                    }
                    page.size_ = temp;
                }
                disk.update(page, entry.pos_);
                break;
            }

            default:
                throw std::runtime_error("WAL recovery: unknown log type");
        }

        if (bad) {
            break;
        }
    }

    wal.close();

    if (std::remove(wal_file.c_str()) != 0) {
        throw std::runtime_error("WAL recovery: failed to remove WAL file");
    }
}

} // namespace sjtu

#endif // LOG_HPP