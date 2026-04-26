#ifndef BUFFER_HPP
#define BUFFER_HPP

#include <list>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "config.hpp"
#include "page.hpp"
#include "disk.hpp"
#include "guard.hpp"

namespace sjtu {
#define BUFFER_MANAGER_TYPE BufferManager<KeyType, ValueType>
#define BUFFER_MANAGER_TEMPLATE_ARGS template<typename KeyType, typename ValueType>

template<typename KeyType, typename ValueType>
class ReadGuard;

template<typename KeyType, typename ValueType>
class WriteGuard;

BUFFER_MANAGER_TEMPLATE_ARGS
class BufferManager {
    friend class ReadGuard<KeyType, ValueType>;
    friend class WriteGuard<KeyType, ValueType>;

private:
    struct CacheEntry {
        diskpos_t pos_;
        std::shared_ptr<PAGE_TYPE> page_;
        bool dirty_;
        size_t pin_count_;
        typename std::list<diskpos_t>::iterator lru_it_;
        std::shared_mutex rwlatch_;
    };
    DiskManager<PAGE_TYPE> disk_;
    std::unordered_map<diskpos_t, CacheEntry> cache_;
    std::list<diskpos_t> lru_list_;
    size_t cache_capacity_;

    mutable std::mutex meta_latch_;
    mutable std::mutex io_latch_;

    void evict_nolock();
    void promote_nolock(diskpos_t pos);
    void load_nolock(diskpos_t pos);
    void load_metadata_nolock(diskpos_t pos, const std::shared_ptr<PAGE_TYPE>& page_ptr);

    std::shared_ptr<PAGE_TYPE> read_page_from_disk(diskpos_t pos);
    void write_page_to_disk(diskpos_t pos, const PAGE_TYPE& page);

    void release_read(diskpos_t pos);
    void release_write(diskpos_t pos);

public:
    BufferManager(size_t cache_capacity = CACHE_CAPACITY, const std::string& file_name = "default.dat");
    BufferManager(const BufferManager& oth) = delete;
    ~BufferManager();

    BufferManager& operator=(const BufferManager& oth) = delete;

    ReadGuard<KeyType, ValueType> read_page(diskpos_t pos);
    WriteGuard<KeyType, ValueType> write_page(diskpos_t pos);
    diskpos_t insert_page(PAGE_TYPE& page);

    void flush();

    diskpos_t get_root_pos();
    void set_root_pos(diskpos_t pos);

};

BUFFER_MANAGER_TEMPLATE_ARGS
BUFFER_MANAGER_TYPE::BufferManager(size_t cache_capacity, const std::string& file_name) : cache_capacity_(cache_capacity) {
    disk_.initialise(file_name);
}

BUFFER_MANAGER_TEMPLATE_ARGS
BUFFER_MANAGER_TYPE::~BufferManager() {
    flush();
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::evict_nolock() {
    if (lru_list_.empty()) {
        return;
    }
    for (auto rit = lru_list_.rbegin(); rit != lru_list_.rend(); rit++) {
        diskpos_t cand = *rit;
        auto it = cache_.find(cand);
        if (it != cache_.end()) {
            if (it->second.pin_count_ > 0) {
                continue;
            }
            if (it->second.dirty_) {
                std::lock_guard<std::mutex> io_lock(io_latch_);
                disk_.update(*(it->second.page_), cand);
            }
            lru_list_.erase(std::next(rit).base());
            cache_.erase(it);
            return;
        }
    }
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::promote_nolock(diskpos_t pos) {
    auto it = cache_.find(pos);
    if (it != cache_.end()) {
        lru_list_.erase(it->second.lru_it_);
        lru_list_.push_front(pos);
        it->second.lru_it_ = lru_list_.begin();
    }
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::load_nolock(diskpos_t pos) {
    auto page_ptr = read_page_from_disk(pos);
    load_metadata_nolock(pos, page_ptr);
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::load_metadata_nolock(diskpos_t pos, const std::shared_ptr<PAGE_TYPE>& page_ptr) {
    lru_list_.push_front(pos);

    auto [it, inserted] = cache_.try_emplace(pos);
    auto &entry = it->second;
    entry.pos_ = pos;
    entry.page_ = page_ptr;
    entry.pin_count_ = 0;
    entry.dirty_ = false;
    entry.lru_it_ = lru_list_.begin();
}

BUFFER_MANAGER_TEMPLATE_ARGS
std::shared_ptr<PAGE_TYPE> BUFFER_MANAGER_TYPE::read_page_from_disk(diskpos_t pos) {
    std::lock_guard<std::mutex> io_lock(io_latch_);
    auto page = std::make_shared<PAGE_TYPE>();
    disk_.read(*page, pos);
    return page;
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::write_page_to_disk(diskpos_t pos, const PAGE_TYPE& page) {
    std::lock_guard<std::mutex> io_lock(io_latch_);
    disk_.update(page, pos);
}

BUFFER_MANAGER_TEMPLATE_ARGS
ReadGuard<KeyType, ValueType> BUFFER_MANAGER_TYPE::read_page(diskpos_t pos) {
    std::shared_ptr<const PAGE_TYPE> page;
    CacheEntry *entry_ptr = nullptr;

    {
        std::lock_guard<std::mutex> lock(meta_latch_);
        auto it = cache_.find(pos);
        if (it != cache_.end()) {
            promote_nolock(pos);
            it->second.pin_count_++;
            page = it->second.page_;
            entry_ptr = &(it->second);
        }
        else {
            if (cache_.size() >= cache_capacity_) {
                evict_nolock();
            }
        }
    }

    if (!entry_ptr) {
        auto disk_page = read_page_from_disk(pos);

        std::lock_guard<std::mutex> lock(meta_latch_);
        auto it = cache_.find(pos);
        if (it != cache_.end()) {
            promote_nolock(pos);
            it->second.pin_count_++;
            page = it->second.page_;
            entry_ptr = &(it->second);
        }
        else {
            if (cache_.size() >= cache_capacity_) {
                evict_nolock();
            }
            load_metadata_nolock(pos, disk_page);
            auto& new_entry = cache_[pos];
            new_entry.pin_count_ = 1;
            page = new_entry.page_;
            entry_ptr = &new_entry;
        }
    }

    entry_ptr->rwlatch_.lock_shared();
    return ReadGuard<KeyType, ValueType>(page, this, pos);
}

BUFFER_MANAGER_TEMPLATE_ARGS
WriteGuard<KeyType, ValueType> BUFFER_MANAGER_TYPE::write_page(diskpos_t pos) {
    std::shared_ptr<PAGE_TYPE> page;
    CacheEntry *entry_ptr = nullptr;

    {
        std::lock_guard<std::mutex> lock(meta_latch_);
        auto it = cache_.find(pos);
        if (it != cache_.end()) {
            promote_nolock(pos);
            it->second.pin_count_++;
            page = it->second.page_;
            entry_ptr = &(it->second);
        }
        else {
            if (cache_.size() >= cache_capacity_) {
                evict_nolock();
            }
        }
    }

    if (!entry_ptr) {
        auto disk_page = read_page_from_disk(pos);

        std::lock_guard<std::mutex> lock(meta_latch_);
        auto it = cache_.find(pos);
        if (it != cache_.end()) {
            promote_nolock(pos);
            it->second.pin_count_++;
            page = it->second.page_;
            entry_ptr = &(it->second);
        }
        else {
            if (cache_.size() >= cache_capacity_) {
                evict_nolock();
            }
            load_metadata_nolock(pos, disk_page);
            auto& new_entry = cache_[pos];
            new_entry.pin_count_ = 1;
            page = new_entry.page_;
            entry_ptr = &new_entry;
        }
    }

    entry_ptr->rwlatch_.lock();
    entry_ptr->dirty_ = true;
    return WriteGuard<KeyType, ValueType>(page, this, pos);
}

BUFFER_MANAGER_TEMPLATE_ARGS
diskpos_t BUFFER_MANAGER_TYPE::insert_page(PAGE_TYPE &page) {
    diskpos_t pos;
    {
        std::lock_guard<std::mutex> io_lock(io_latch_);
        pos = disk_.write(page);
    }
    auto page_ptr = std::make_shared<PAGE_TYPE>(page);

    std::lock_guard<std::mutex> lock(meta_latch_);
    if (cache_.size() >= cache_capacity_) {
        evict_nolock();
    }
    load_metadata_nolock(pos, page_ptr);
    return pos;
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::release_read(diskpos_t pos) {
    std::lock_guard<std::mutex> lock(meta_latch_);
    auto it = cache_.find(pos);
    if (it != cache_.end()) {
        it->second.rwlatch_.unlock_shared();
        it->second.pin_count_--;
    }
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::release_write(diskpos_t pos) {
    std::lock_guard<std::mutex> lock(meta_latch_);
    auto it = cache_.find(pos);
    if (it != cache_.end()) {
        it->second.rwlatch_.unlock();
        it->second.pin_count_--;
    }
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::flush() {
    std::lock_guard<std::mutex> lock(meta_latch_);
    
    for (auto& pair : cache_) {
        if (pair.second.dirty_) {
            std::lock_guard<std::mutex> io_lock(io_latch_);
            disk_.update(*(pair.second.page_), pair.first);
            pair.second.dirty_ = false;
        }
    }
    cache_.clear();
    lru_list_.clear();
}

BUFFER_MANAGER_TEMPLATE_ARGS
diskpos_t BUFFER_MANAGER_TYPE::get_root_pos() {
    std::lock_guard<std::mutex> meta_lock(meta_latch_);
    std::lock_guard<std::mutex> io_lock(io_latch_);
    diskpos_t root_pos;
    disk_.get_info(root_pos, 2);
    return root_pos;
}

BUFFER_MANAGER_TEMPLATE_ARGS
void BUFFER_MANAGER_TYPE::set_root_pos(diskpos_t pos) {
    std::lock_guard<std::mutex> meta_lock(meta_latch_);
    std::lock_guard<std::mutex> io_lock(io_latch_);
    disk_.write_info(pos, 2);
}

} // namespace sjtu

#endif // BUFFER_HPP