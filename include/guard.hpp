#ifndef GUARD_HPP
#define GUARD_HPP

#include <memory>
#include "page.hpp"

namespace sjtu {
#define READ_GUARD_TYPE ReadGuard<KeyType, ValueType>
#define READ_GUARD_TEMPLATE_ARGS template<typename KeyType, typename ValueType>

#define WRITE_GUARD_TYPE WriteGuard<KeyType, ValueType>
#define WRITE_GUARD_TEMPLATE_ARGS template<typename KeyType, typename ValueType>

template<typename KeyType, typename ValueType>
class BufferManager;

READ_GUARD_TEMPLATE_ARGS
class ReadGuard {
    friend class BufferManager<KeyType, ValueType>;

private:
    std::shared_ptr<const PAGE_TYPE> page_;
    BufferManager<KeyType, ValueType>* bpm_;
    diskpos_t pos_;

    ReadGuard(std::shared_ptr<const PAGE_TYPE> page, BufferManager<KeyType, ValueType>* bpm, diskpos_t pos)
        : page_(page), bpm_(bpm), pos_(pos) {}

public:
    ReadGuard(const ReadGuard& oth) = delete;
    ReadGuard& operator=(const ReadGuard& oth) = delete;

    ReadGuard(ReadGuard&& oth) noexcept: page_(oth.page_), bpm_(oth.bpm_), pos_(oth.pos_) { oth.bpm_ = nullptr; }
    ReadGuard& operator=(ReadGuard&& oth) noexcept {
        if (this == &oth) {
            return *this;
        }
        if (bpm_) {
            bpm_->release_read(pos_);
        }
        this->page_ = oth.page_;
        this->bpm_ = oth.bpm_;
        this->pos_ = oth.pos_;
        oth.bpm_ = nullptr;
        return *this;
    }

    diskpos_t get_pos() const { return pos_; }
    const PAGE_TYPE& get_page() const { return *page_; }
    const PAGE_TYPE* operator->() const { return page_.get(); }
    const PAGE_TYPE& operator*() const { return *page_; }

    ~ReadGuard() {
        if (bpm_) {
            bpm_->release_read(pos_);
            bpm_ = nullptr;
        }
    }

};

WRITE_GUARD_TEMPLATE_ARGS
class WriteGuard {
    friend class BufferManager<KeyType, ValueType>;

    private:
    std::shared_ptr<PAGE_TYPE> page_;
    BufferManager<KeyType, ValueType>* bpm_;
    diskpos_t pos_;

    WriteGuard(std::shared_ptr<PAGE_TYPE> page, BufferManager<KeyType, ValueType>* bpm, diskpos_t pos)
        : page_(page), bpm_(bpm), pos_(pos) {}

    public:
    WriteGuard(const WriteGuard& oth) = delete;
    WriteGuard& operator=(const WriteGuard& oth) = delete;

    WriteGuard(WriteGuard&& oth) noexcept: page_(oth.page_), bpm_(oth.bpm_), pos_(oth.pos_) { oth.bpm_ = nullptr; }
    WriteGuard& operator=(WriteGuard&& oth) noexcept {
        if (this == &oth) {
            return *this;
        }
        if (bpm_) {
            bpm_->release_write(pos_);
        }
        this->page_ = oth.page_;
        this->bpm_ = oth.bpm_;
        this->pos_ = oth.pos_;
        oth.bpm_ = nullptr;
        return *this;
    }

    diskpos_t get_pos() { return pos_; }
    PAGE_TYPE& get_page() { return *page_; }
    PAGE_TYPE* operator->() { return page_.get(); }
    PAGE_TYPE& operator*() { return *page_; }

    ~WriteGuard() {
        if (bpm_) {
            bpm_->release_write(pos_);
            bpm_ = nullptr;
        }
    }

};

} // namespace sjtu

#endif // GUARD_HPP