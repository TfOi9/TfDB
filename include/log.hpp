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
#include <unordered_map>
#include <cstring>
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
    using log_type_t = uint8_t;

    struct LogType {
        static constexpr log_type_t PageUpdate    = 0;
        static constexpr log_type_t RootUpdate    = 1;
        static constexpr log_type_t MetaUpdate    = 2;
        static constexpr log_type_t SlotInsert    = 3;
        static constexpr log_type_t SlotDelete    = 4;
        static constexpr log_type_t SlotUpdate    = 5;
        static constexpr log_type_t SlotRangeInit = 6;
        static constexpr log_type_t PageInit      = 7;
    };

    struct MetaField {
        static constexpr uint8_t NONE  = 0;
        static constexpr uint8_t SIZE  = 1 << 0;
        static constexpr uint8_t LEFT  = 1 << 1;
        static constexpr uint8_t RIGHT = 1 << 2;
        static constexpr uint8_t FA    = 1 << 3;
        static constexpr uint8_t TYPE  = 1 << 4;
    };

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
    void append_page_init(diskpos_t pos, const PAGE_TYPE& page);
    void append_meta_update(diskpos_t pos, uint8_t field_mask, const PAGE_TYPE& page);
    void append_slot_insert(diskpos_t pos, uint16_t slot_idx, diskpos_t child_pos, const KeyType& key, const ValueType& val);
    void append_slot_delete(diskpos_t pos, uint16_t slot_idx);
    void append_slot_update(diskpos_t pos, uint16_t slot_idx, const KeyType& key, const ValueType& val);
    void append_slot_range_init(diskpos_t pos, uint16_t count, bool is_leaf, const PAGE_TYPE& src_page);

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
    lsn_counter_.store(1, std::memory_order_relaxed);

    fd_ = open(file_name_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open WAL file");
    }
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_page_update(diskpos_t pos, const PAGE_TYPE& page) {
    if (!USE_WAL) return;
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

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_page_init(diskpos_t pos, const PAGE_TYPE& page) {
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

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_meta_update(diskpos_t pos, uint8_t field_mask,
                                          const PAGE_TYPE& page) {
    if (!USE_WAL) return;
    if (field_mask == MetaField::NONE) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    size_t payload_size = 1;
    if (field_mask & MetaField::SIZE)  payload_size += sizeof(size_t);
    if (field_mask & MetaField::LEFT)  payload_size += sizeof(diskpos_t);
    if (field_mask & MetaField::RIGHT) payload_size += sizeof(diskpos_t);
    if (field_mask & MetaField::FA)    payload_size += sizeof(diskpos_t);
    if (field_mask & MetaField::TYPE)  payload_size += sizeof(PageType);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::MetaUpdate,
        .pos_ = pos,
        .size_ = payload_size
    };

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&field_mask, sizeof(field_mask));

    if (field_mask & MetaField::SIZE)  append_bytes(&page.size_, sizeof(size_t));
    if (field_mask & MetaField::LEFT)  append_bytes(&page.left_, sizeof(diskpos_t));
    if (field_mask & MetaField::RIGHT) append_bytes(&page.right_, sizeof(diskpos_t));
    if (field_mask & MetaField::FA)    append_bytes(&page.fa_, sizeof(diskpos_t));
    if (field_mask & MetaField::TYPE)  append_bytes(&page.type_, sizeof(PageType));
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_insert(diskpos_t pos, uint16_t slot_idx,
                                          diskpos_t child_pos,
                                          const KeyType& key, const ValueType& val) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    constexpr size_t kFixedSize = sizeof(uint16_t) + sizeof(diskpos_t);
    const size_t payload_size = kFixedSize + sizeof(KeyType) + sizeof(ValueType);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotInsert,
        .pos_ = pos,
        .size_ = payload_size
    };

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&slot_idx, sizeof(slot_idx));
    append_bytes(&child_pos, sizeof(child_pos));
    append_bytes(&key, sizeof(KeyType));
    append_bytes(&val, sizeof(ValueType));
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_delete(diskpos_t pos, uint16_t slot_idx) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    constexpr size_t payload_size = sizeof(uint16_t);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotDelete,
        .pos_ = pos,
        .size_ = payload_size
    };

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&slot_idx, sizeof(slot_idx));
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_update(diskpos_t pos, uint16_t slot_idx,
                                          const KeyType& key, const ValueType& val) {
    if (!USE_WAL) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    const size_t payload_size = sizeof(uint16_t) + sizeof(KeyType) + sizeof(ValueType);

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotUpdate,
        .pos_ = pos,
        .size_ = payload_size
    };

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&slot_idx, sizeof(slot_idx));
    append_bytes(&key, sizeof(KeyType));
    append_bytes(&val, sizeof(ValueType));
}

LOGMANAGER_TEMPLATE_ARGS
void LOGMANAGER_TYPE::append_slot_range_init(diskpos_t pos, uint16_t count,
                                              bool is_leaf,
                                              const PAGE_TYPE& src_page) {
    if (!USE_WAL) return;
    if (count == 0) return;
    std::lock_guard<std::mutex> lock(log_mtx_);

    size_t payload_size = sizeof(uint16_t) + sizeof(uint8_t);
    payload_size += static_cast<size_t>(count) * sizeof(KeyType);
    payload_size += static_cast<size_t>(count) * sizeof(ValueType);
    if (!is_leaf) {
        payload_size += static_cast<size_t>(count) * sizeof(diskpos_t);
    }

    LogEntry entry {
        .lsn_ = lsn_counter_.fetch_add(1),
        .type_ = LogType::SlotRangeInit,
        .pos_ = pos,
        .size_ = payload_size
    };

    const uint8_t is_leaf_byte = is_leaf ? 1 : 0;

    append_bytes(&entry, sizeof(LogEntry));
    append_bytes(&count, sizeof(count));
    append_bytes(&is_leaf_byte, sizeof(is_leaf_byte));

    for (uint16_t i = 0; i < count; ++i) {
        append_bytes(&src_page.data_[i], sizeof(KeyType));
    }
    for (uint16_t i = 0; i < count; ++i) {
        append_bytes(&src_page.data_[i].val_, sizeof(ValueType));
    }
    if (!is_leaf) {
        for (uint16_t i = 0; i < count; ++i) {
            append_bytes(&src_page.ch_[i], sizeof(diskpos_t));
        }
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
            ssize_t written = write(fd_,
                reinterpret_cast<const char*>(buffer_.data() + offset),
                buffer_.size() - offset);
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

    std::unordered_map<diskpos_t, PAGE_TYPE> page_cache;

    auto read_exact = [&](void *dst, size_t size) -> bool {
        wal.read(reinterpret_cast<char *>(dst),
                 static_cast<std::streamsize>(size));
        return wal.gcount() == static_cast<std::streamsize>(size);
    };

    auto skip_bytes = [&](size_t bytes) {
        wal.seekg(static_cast<std::streamoff>(bytes), std::ios::cur);
    };

    auto meta_payload_size = [](uint8_t mask) -> size_t {
        size_t sz = 1;
        if (mask & MetaField::SIZE)  sz += sizeof(size_t);
        if (mask & MetaField::LEFT)  sz += sizeof(diskpos_t);
        if (mask & MetaField::RIGHT) sz += sizeof(diskpos_t);
        if (mask & MetaField::FA)    sz += sizeof(diskpos_t);
        if (mask & MetaField::TYPE)  sz += sizeof(PageType);
        return sz;
    };

    auto slot_range_payload_size = [](uint16_t count, bool is_leaf) -> size_t {
        size_t sz = sizeof(uint16_t) + sizeof(uint8_t);
        sz += static_cast<size_t>(count) * sizeof(KeyType);
        sz += static_cast<size_t>(count) * sizeof(ValueType);
        if (!is_leaf) {
            sz += static_cast<size_t>(count) * sizeof(diskpos_t);
        }
        return sz;
    };

    auto load_page = [&](diskpos_t pos) -> PAGE_TYPE* {
        auto it = page_cache.find(pos);
        if (it != page_cache.end()) return &it->second;
        PAGE_TYPE pg;
        disk.read(pg, pos);
        auto [iter, _] = page_cache.emplace(pos, std::move(pg));
        return &iter->second;
    };

    constexpr size_t kHeaderSize = sizeof(LogEntry);

    while (wal.good()) {
        LogEntry entry;
        wal.read(reinterpret_cast<char*>(&entry), kHeaderSize);
        std::streamsize header_bytes = wal.gcount();
        if (header_bytes == 0) break;
        if (header_bytes != static_cast<std::streamsize>(kHeaderSize)) break;

        switch (entry.type_) {

        case LogType::PageUpdate: {
            if (entry.size_ != sizeof(PAGE_TYPE)) {
                throw std::runtime_error("WAL recovery: PageUpdate has invalid size");
            }
            PAGE_TYPE page;
            if (!read_exact(&page, sizeof(PAGE_TYPE))) break;
            page.page_lsn_ = entry.lsn_;
            disk.update(page, entry.pos_);
            break;
        }

        case LogType::RootUpdate: {
            if (entry.size_ != 0) {
                throw std::runtime_error("WAL recovery: RootUpdate has invalid size");
            }
            diskpos_t root = entry.pos_;
            disk.write_info(root, 2);
            break;
        }

        case LogType::PageInit: {
            if (entry.size_ != sizeof(PAGE_TYPE)) {
                throw std::runtime_error("WAL recovery: PageInit has invalid size");
            }
            PAGE_TYPE page;
            if (!read_exact(&page, sizeof(PAGE_TYPE))) break;
            page.page_lsn_ = entry.lsn_;
            disk.update(page, entry.pos_);
            break;
        }

        case LogType::MetaUpdate: {
            uint8_t field_mask;
            if (!read_exact(&field_mask, sizeof(field_mask))) break;

            size_t expected = meta_payload_size(field_mask);
            if (entry.size_ != expected) {
                throw std::runtime_error("WAL recovery: MetaUpdate has invalid size");
            }

            PAGE_TYPE* page_ptr = load_page(entry.pos_);
            if (page_ptr->page_lsn_ >= entry.lsn_) {
                skip_bytes( entry.size_ - 1);
                break;
            }

            if (field_mask & MetaField::SIZE) {
                size_t val;
                if (!read_exact(&val, sizeof(val))) break;
                page_ptr->size_ = val;
            }
            if (field_mask & MetaField::LEFT) {
                diskpos_t val;
                if (!read_exact(&val, sizeof(val))) break;
                page_ptr->left_ = val;
            }
            if (field_mask & MetaField::RIGHT) {
                diskpos_t val;
                if (!read_exact(&val, sizeof(val))) break;
                page_ptr->right_ = val;
            }
            if (field_mask & MetaField::FA) {
                diskpos_t val;
                if (!read_exact(&val, sizeof(val))) break;
                page_ptr->fa_ = val;
            }
            if (field_mask & MetaField::TYPE) {
                PageType val;
                if (!read_exact(&val, sizeof(val))) break;
                page_ptr->type_ = val;
            }

            page_ptr->page_lsn_ = entry.lsn_;
            break;
        }

        case LogType::SlotInsert: {
            constexpr size_t kFixedSize = sizeof(uint16_t) + sizeof(diskpos_t);
            constexpr size_t kPayloadSize =
                kFixedSize + sizeof(KeyType) + sizeof(ValueType);
            if (entry.size_ != kPayloadSize) {
                throw std::runtime_error("WAL recovery: SlotInsert has invalid size");
            }

            PAGE_TYPE* page_ptr = load_page(entry.pos_);
            if (page_ptr->page_lsn_ >= entry.lsn_) {
                skip_bytes( kPayloadSize);
                break;
            }

            uint16_t slot_idx;
            diskpos_t child_pos;
            KeyType key;
            ValueType val;
            if (!read_exact(&slot_idx, sizeof(slot_idx))) break;
            if (!read_exact(&child_pos, sizeof(child_pos))) break;
            if (!read_exact(&key, sizeof(KeyType))) break;
            if (!read_exact(&val, sizeof(ValueType))) break;

            for (size_t i = page_ptr->size_; i > slot_idx; --i) {
                page_ptr->data_[i] = page_ptr->data_[i - 1];
                page_ptr->ch_[i] = page_ptr->ch_[i - 1];
            }
            page_ptr->data_[slot_idx].key_ = key;
            page_ptr->data_[slot_idx].val_ = val;
            page_ptr->ch_[slot_idx] = child_pos;
            page_ptr->size_++;

            page_ptr->page_lsn_ = entry.lsn_;
            break;
        }

        case LogType::SlotDelete: {
            constexpr size_t kPayloadSize = sizeof(uint16_t);
            if (entry.size_ != kPayloadSize) {
                throw std::runtime_error("WAL recovery: SlotDelete has invalid size");
            }

            PAGE_TYPE* page_ptr = load_page(entry.pos_);
            if (page_ptr->page_lsn_ >= entry.lsn_) {
                skip_bytes( kPayloadSize);
                break;
            }

            uint16_t slot_idx;
            if (!read_exact( &slot_idx, sizeof(slot_idx))) break;

            for (size_t i = slot_idx; i + 1 < page_ptr->size_; ++i) {
                page_ptr->data_[i] = page_ptr->data_[i + 1];
                page_ptr->ch_[i] = page_ptr->ch_[i + 1];
            }
            page_ptr->size_--;

            page_ptr->page_lsn_ = entry.lsn_;
            break;
        }

        case LogType::SlotUpdate: {
            constexpr size_t kPayloadSize =
                sizeof(uint16_t) + sizeof(KeyType) + sizeof(ValueType);
            if (entry.size_ != kPayloadSize) {
                throw std::runtime_error("WAL recovery: SlotUpdate has invalid size");
            }

            PAGE_TYPE* page_ptr = load_page(entry.pos_);
            if (page_ptr->page_lsn_ >= entry.lsn_) {
                skip_bytes( kPayloadSize);
                break;
            }

            uint16_t slot_idx;
            KeyType key;
            ValueType val;
            if (!read_exact(&slot_idx, sizeof(slot_idx))) break;
            if (!read_exact(&key, sizeof(KeyType))) break;
            if (!read_exact(&val, sizeof(ValueType))) break;

            page_ptr->data_[slot_idx].key_ = key;
            page_ptr->data_[slot_idx].val_ = val;

            page_ptr->page_lsn_ = entry.lsn_;
            break;
        }

        case LogType::SlotRangeInit: {
            uint16_t count;
            uint8_t is_leaf_byte;
            if (!read_exact(&count, sizeof(count))) break;
            if (!read_exact(&is_leaf_byte, sizeof(is_leaf_byte))) break;

            const bool is_leaf = (is_leaf_byte != 0);
            size_t expected = slot_range_payload_size(count, is_leaf);
            if (entry.size_ != expected) {
                throw std::runtime_error("WAL recovery: SlotRangeInit has invalid size");
            }

            PAGE_TYPE* page_ptr = load_page(entry.pos_);
            if (page_ptr->page_lsn_ >= entry.lsn_) {
                size_t remaining = entry.size_ - sizeof(count) - sizeof(is_leaf_byte);
                skip_bytes( remaining);
                break;
            }

            const size_t base = page_ptr->size_;
            for (size_t i = 0; i < count; ++i) {
                if (!read_exact(&page_ptr->data_[base + i].key_, sizeof(KeyType))) break;
            }
            for (size_t i = 0; i < count; ++i) {
                if (!read_exact(&page_ptr->data_[base + i].val_, sizeof(ValueType))) break;
            }
            if (!is_leaf) {
                for (size_t i = 0; i < count; ++i) {
                    if (!read_exact(&page_ptr->ch_[base + i], sizeof(diskpos_t))) break;
                }
            }

            page_ptr->size_ += count;
            page_ptr->page_lsn_ = entry.lsn_;
            break;
        }

        default: {
            if (entry.size_ > 0) {
                skip_bytes( entry.size_);
            }
            break;
        }

        } // switch
    }

    wal.close();

    for (auto &kv : page_cache) {
        disk.update(kv.second, kv.first);
    }

    if (std::remove(wal_file.c_str()) != 0) {
        throw std::runtime_error("WAL recovery: failed to remove WAL file");
    }
}

} // namespace sjtu

#endif // LOG_HPP
