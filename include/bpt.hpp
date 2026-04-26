#ifndef BPT_HPP
#define BPT_HPP

#include <optional>
#include <string>
#include <vector>

#include "config.hpp"
#include "page.hpp"
#include "buffer.hpp"

namespace sjtu {
#define BPT_TYPE BPlusTree<KeyType, ValueType>
#define BPT_TEMPLATE_ARGS template<typename KeyType, typename ValueType>

BPT_TEMPLATE_ARGS
class BPlusTree {
private:
    BUFFER_MANAGER_TYPE buffer_;
    PAGE_TYPE cur_;
    diskpos_t pos_ = 0;
    diskpos_t root_ = 0;

    void read_page_copy(diskpos_t pos, PAGE_TYPE &page);

    void write_page_copy(diskpos_t pos, const PAGE_TYPE &page);

    void split();

    bool borrowl();

    bool borrowr();

    void merge();

    void balance();

public:
    BPlusTree(const std::string file_name = "bpt.dat");

    ~BPlusTree();

    std::optional<ValueType> find(const KeyType& key);

    void find_all(const KeyType& key, std::vector<ValueType>& vec);

    void insert(const KeyType& key, const ValueType& val);

    void erase(const KeyType& key, const ValueType& val);

};

BPT_TEMPLATE_ARGS
BPT_TYPE::BPlusTree(const std::string file_name) : buffer_(CACHE_CAPACITY, file_name) {
    root_ = buffer_.get_root_pos();
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::read_page_copy(diskpos_t pos, PAGE_TYPE &page) {
    auto guard = buffer_.read_page(pos);
    page = guard.get_page();
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::write_page_copy(diskpos_t pos, const PAGE_TYPE &page) {
    auto guard = buffer_.write_page(pos);
    guard.get_page() = page;
}

BPT_TEMPLATE_ARGS
BPT_TYPE::~BPlusTree() {
    buffer_.set_root_pos(root_);
}

BPT_TEMPLATE_ARGS
std::optional<ValueType> BPT_TYPE::find(const KeyType& key) {
    if (root_ == 0) {
        return std::nullopt;
    }
    pos_ = root_;
    read_page_copy(pos_, cur_);
    while (cur_.type_ != PageType::Leaf) {
        int k = cur_.lower_bound(key);
        pos_ = cur_.ch_[k];
        read_page_copy(pos_, cur_);
    }
    int k = cur_.lower_bound(key);
    if (cur_.data_[k].key_ != key) {
        return std::nullopt;
    }
    return cur_.data_[k].val_;
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::find_all(const KeyType& key, std::vector<ValueType>& vec) {
    vec.clear();
    if (root_ == 0) {
        return;
    }
    pos_ = root_;
    read_page_copy(pos_, cur_);
    while (cur_.type_ != PageType::Leaf) {
        int k = cur_.lower_bound(key);
        pos_ = cur_.ch_[k];
        read_page_copy(pos_, cur_);
    }
    int k = cur_.lower_bound(key);
    if (cur_.data_[k].key_ != key) {
        return;
    }
    int curk = k;
    while (cur_.data_[curk].key_ == key) {
        vec.push_back(cur_.data_[curk].val_);
        if (curk < static_cast<int>(cur_.size_) - 1) {
            curk++;
        }
        else {
            if (cur_.right_ == -1) {
                break;
            }
            else {
                pos_ = cur_.right_;
                read_page_copy(pos_, cur_);
                curk = 0;
            }
        }
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::split() {
    PAGE_TYPE newp;
    newp.size_ = PAGE_SLOT_COUNT / 2;
    cur_.size_ = PAGE_SLOT_COUNT / 2;
    if (cur_.type_ == PageType::Leaf) {
        newp.type_ = PageType::Leaf;
    }
    else {
        newp.type_ = PageType::Internal;
    }
    newp.fa_ = cur_.fa_;
    newp.left_ = pos_;
    newp.right_ = cur_.right_;

    if (cur_.type_ == PageType::Leaf) {
        for (int i = 0; i < static_cast<int>(newp.size_); i++) {
            newp.data_[i] = cur_.data_[i + static_cast<int>(newp.size_)];
        }

        KEYPAIR_TYPE split_at = cur_.back();
        KEYPAIR_TYPE max_pair = newp.back();

        if (cur_.fa_ != -1) {
            PAGE_TYPE f;
            read_page_copy(cur_.fa_, f);

            int fa_pos = f.lower_bound(max_pair);
            for (int i = static_cast<int>(f.size_) - 1; i >= fa_pos; i--) {
                f.data_[i + 1] = f.data_[i];
                f.ch_[i + 1] = f.ch_[i];
            }

            diskpos_t newp_pos = buffer_.insert_page(newp);

            f.data_[fa_pos] = split_at;
            f.data_[fa_pos + 1] = max_pair;
            f.ch_[fa_pos] = pos_;
            f.ch_[fa_pos + 1] = newp_pos;
            f.size_++;

            if (cur_.right_ != -1) {
                PAGE_TYPE rp;
                read_page_copy(cur_.right_, rp);
                rp.left_ = newp_pos;
                write_page_copy(cur_.right_, rp);
            }

            cur_.right_ = newp_pos;
            bool need_split_parent = (f.size_ == PAGE_SLOT_COUNT);

            write_page_copy(cur_.fa_, f);
            write_page_copy(pos_, cur_);

            if (need_split_parent) {
                pos_ = cur_.fa_;
                cur_ = f;
                split();
            }
        }
        else {
            PAGE_TYPE newr;
            newr.type_ = PageType::Internal;
            newr.size_ = 2;
            newr.data_[0] = split_at;
            newr.data_[1] = max_pair;
            newr.ch_[0] = pos_;

            diskpos_t newp_pos = buffer_.insert_page(newp);
            newr.ch_[1] = newp_pos;

            cur_.right_ = newp_pos;
            root_ = buffer_.insert_page(newr);

            cur_.fa_ = root_;
            newp.fa_ = root_;

            write_page_copy(pos_, cur_);
            write_page_copy(newp_pos, newp);
        }

        return;
    }

    for (int i = 0; i < static_cast<int>(newp.size_); i++) {
        newp.data_[i] = cur_.data_[i + static_cast<int>(newp.size_)];
        newp.ch_[i] = cur_.ch_[i + static_cast<int>(newp.size_)];
    }

    diskpos_t newp_pos = buffer_.insert_page(newp);

    for (int i = 0; i < static_cast<int>(newp.size_); i++) {
        PAGE_TYPE ch;
        read_page_copy(newp.ch_[i], ch);
        ch.fa_ = newp_pos;
        write_page_copy(newp.ch_[i], ch);
    }

    KEYPAIR_TYPE split_at = cur_.back();
    KEYPAIR_TYPE max_pair = newp.back();

    if (cur_.fa_ != -1) {
        PAGE_TYPE f;
        read_page_copy(cur_.fa_, f);

        int fa_pos = f.lower_bound(max_pair);
        for (int i = static_cast<int>(f.size_) - 1; i >= fa_pos; i--) {
            f.data_[i + 1] = f.data_[i];
            f.ch_[i + 1] = f.ch_[i];
        }

        f.data_[fa_pos] = split_at;
        f.data_[fa_pos + 1] = max_pair;
        f.ch_[fa_pos] = pos_;
        f.ch_[fa_pos + 1] = newp_pos;
        f.size_++;

        if (cur_.right_ != -1) {
            PAGE_TYPE rp;
            read_page_copy(cur_.right_, rp);
            rp.left_ = newp_pos;
            write_page_copy(cur_.right_, rp);
        }

        cur_.right_ = newp_pos;
        bool need_split_parent = (f.size_ == PAGE_SLOT_COUNT);

        write_page_copy(cur_.fa_, f);
        write_page_copy(pos_, cur_);
        write_page_copy(newp_pos, newp);

        if (need_split_parent) {
            pos_ = cur_.fa_;
            cur_ = f;
            split();
        }
    }
    else {
        PAGE_TYPE newr;
        newr.type_ = PageType::Internal;
        newr.size_ = 2;
        newr.data_[0] = split_at;
        newr.data_[1] = max_pair;
        newr.ch_[0] = pos_;
        newr.ch_[1] = newp_pos;

        root_ = buffer_.insert_page(newr);

        cur_.fa_ = root_;
        newp.fa_ = root_;

        write_page_copy(pos_, cur_);
        write_page_copy(newp_pos, newp);
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::insert(const KeyType& key, const ValueType& val) {
    KEYPAIR_TYPE kp(key, val);
    if (root_ == 0) {
        PAGE_TYPE newr;
        newr.size_ = 1;
        newr.type_ = PageType::Leaf;
        newr.data_[0] = kp;
        root_ = buffer_.insert_page(newr);
        return;
    }

    pos_ = root_;
    read_page_copy(pos_, cur_);

    while (cur_.type_ != PageType::Leaf) {
        int k = cur_.lower_bound(kp);
        if (cur_.data_[k] < kp) {
            cur_.data_[k] = kp;
            write_page_copy(pos_, cur_);
        }

        pos_ = cur_.ch_[k];
        read_page_copy(pos_, cur_);
    }

    int k = cur_.lower_bound(kp);
    if (cur_.data_[k] == kp) {
        return;
    }

    if (cur_.data_[k] < kp) {
        cur_.data_[k + 1] = kp;
        cur_.size_++;
    }
    else {
        for (int i = static_cast<int>(cur_.size_) - 1; i >= k; i--) {
            cur_.data_[i + 1] = cur_.data_[i];
        }
        cur_.data_[k] = kp;
        cur_.size_++;
    }

    write_page_copy(pos_, cur_);

    bool need_split = (cur_.size_ == PAGE_SLOT_COUNT);
    if (need_split) {
        split();
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::erase(const KeyType& key, const ValueType& val) {
    if (root_ == 0) {
        return;
    }

    KEYPAIR_TYPE kp(key, val);
    pos_ = root_;

    read_page_copy(pos_, cur_);
    while (cur_.type_ != PageType::Leaf) {
        int k = cur_.lower_bound(kp);
        pos_ = cur_.ch_[k];
        read_page_copy(pos_, cur_);
    }

    int k = cur_.lower_bound(kp);
    if (cur_.data_[k] != kp) {
        return;
    }

    for (int i = k; i < static_cast<int>(cur_.size_) - 1; i++) {
        cur_.data_[i] = cur_.data_[i + 1];
    }

    cur_.size_--;
    write_page_copy(pos_, cur_);

    KEYPAIR_TYPE max_pair = cur_.back();
    diskpos_t fpos = cur_.fa_;

    while (fpos != -1) {
        PAGE_TYPE f;
        read_page_copy(fpos, f);

        int p = f.lower_bound(kp);
        if (f.data_[p] == kp) {
            f.data_[p] = max_pair;
            write_page_copy(fpos, f);
            fpos = f.fa_;
        }
        else {
            break;
        }
    }

    bool need_balance = (cur_.size_ < PAGE_SLOT_COUNT / 2);
    if (need_balance) {
        balance();
    }
}

BPT_TEMPLATE_ARGS
bool BPT_TYPE::borrowl() {
    if (cur_.fa_ == -1 || cur_.size_ == 0) {
        return false;
    }

    diskpos_t fpos = cur_.fa_;
    KEYPAIR_TYPE max_pair = cur_.back();

    PAGE_TYPE f;
    read_page_copy(fpos, f);

    int k = f.lower_bound(max_pair);
    if (k == 0) {
        return false;
    }

    diskpos_t bpos = f.ch_[k - 1];
    PAGE_TYPE bro;
    read_page_copy(bpos, bro);

    if (bro.size_ <= PAGE_SLOT_COUNT / 2) {
        return false;
    }

    for (int i = static_cast<int>(cur_.size_) - 1; i >= 0; i--) {
        cur_.data_[i + 1] = cur_.data_[i];
        cur_.ch_[i + 1] = cur_.ch_[i];
    }

    cur_.data_[0] = bro.back();
    cur_.ch_[0] = bro.ch_[bro.size_ - 1];
    cur_.size_++;
    bro.size_--;

    if (cur_.type_ == PageType::Internal) {
        PAGE_TYPE son;
        read_page_copy(cur_.ch_[0], son);
        son.fa_ = pos_;
        write_page_copy(cur_.ch_[0], son);
    }

    f.data_[k - 1] = bro.back();

    write_page_copy(pos_, cur_);
    write_page_copy(bpos, bro);
    write_page_copy(fpos, f);

    return true;
}

BPT_TEMPLATE_ARGS
bool BPT_TYPE::borrowr() {
    if (cur_.fa_ == -1 || cur_.size_ == 0) {
        return false;
    }

    diskpos_t fpos = cur_.fa_;
    KEYPAIR_TYPE max_pair = cur_.back();

    PAGE_TYPE f;
    read_page_copy(fpos, f);

    int k = f.lower_bound(max_pair);
    if (k == static_cast<int>(f.size_) - 1) {
        return false;
    }

    diskpos_t bpos = f.ch_[k + 1];
    PAGE_TYPE bro;
    read_page_copy(bpos, bro);

    if (bro.size_ <= PAGE_SLOT_COUNT / 2) {
        return false;
    }

    cur_.data_[cur_.size_] = bro.data_[0];
    cur_.ch_[cur_.size_] = bro.ch_[0];
    cur_.size_++;

    for (int i = 0; i < static_cast<int>(bro.size_) - 1; i++) {
        bro.data_[i] = bro.data_[i + 1];
        bro.ch_[i] = bro.ch_[i + 1];
    }

    bro.size_--;

    if (cur_.type_ == PageType::Internal) {
        PAGE_TYPE son;
        read_page_copy(cur_.ch_[cur_.size_ - 1], son);
        son.fa_ = pos_;
        write_page_copy(cur_.ch_[cur_.size_ - 1], son);
    }

    f.data_[k] = cur_.back();

    write_page_copy(pos_, cur_);
    write_page_copy(bpos, bro);
    write_page_copy(fpos, f);

    return true;
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::merge() {
    if (cur_.fa_ == -1) {
        return;
    }

    KEYPAIR_TYPE max_pair = cur_.back();
    diskpos_t fpos = cur_.fa_;

    PAGE_TYPE f;
    read_page_copy(fpos, f);

    int k = f.lower_bound(max_pair);

    if (k) {
        diskpos_t bpos = f.ch_[k - 1];
        PAGE_TYPE bro;
        read_page_copy(bpos, bro);

        if (cur_.type_ == PageType::Internal) {
            for (int i = 0; i < static_cast<int>(cur_.size_); i++) {
                PAGE_TYPE son;
                read_page_copy(cur_.ch_[i], son);
                son.fa_ = bpos;
                write_page_copy(cur_.ch_[i], son);
            }
        }

        for (int i = 0; i < static_cast<int>(cur_.size_); i++) {
            bro.data_[bro.size_ + i] = cur_.data_[i];
            bro.ch_[bro.size_ + i] = cur_.ch_[i];
        }

        bro.size_ += cur_.size_;
        cur_.size_ = 0;
        bro.right_ = cur_.right_;

        if (cur_.right_ != -1) {
            PAGE_TYPE rp;
            read_page_copy(cur_.right_, rp);
            rp.left_ = bpos;
            write_page_copy(cur_.right_, rp);
        }

        for (int i = k; i < static_cast<int>(f.size_) - 1; i++) {
            f.data_[i] = f.data_[i + 1];
            f.ch_[i] = f.ch_[i + 1];
        }

        f.size_--;
        f.data_[k - 1] = bro.back();

        write_page_copy(bpos, bro);
        write_page_copy(pos_, cur_);
        write_page_copy(fpos, f);

        bool need_balance = (f.size_ < PAGE_SLOT_COUNT / 2);
        if (need_balance) {
            pos_ = fpos;
            cur_ = f;
            balance();
        }
    }
    else if (k != static_cast<int>(f.size_) - 1) {
        diskpos_t bpos = f.ch_[k + 1];
        PAGE_TYPE bro;
        read_page_copy(bpos, bro);

        if (cur_.type_ == PageType::Internal) {
            for (int i = 0; i < static_cast<int>(bro.size_); i++) {
                PAGE_TYPE son;
                read_page_copy(bro.ch_[i], son);
                son.fa_ = pos_;
                write_page_copy(bro.ch_[i], son);
            }
        }

        for (int i = 0; i < static_cast<int>(bro.size_); i++) {
            cur_.data_[cur_.size_ + i] = bro.data_[i];
            cur_.ch_[cur_.size_ + i] = bro.ch_[i];
        }

        cur_.size_ += bro.size_;
        bro.size_ = 0;
        cur_.right_ = bro.right_;

        if (bro.right_ != -1) {
            PAGE_TYPE rp;
            read_page_copy(bro.right_, rp);
            rp.left_ = pos_;
            write_page_copy(bro.right_, rp);
        }

        for (int i = k + 1; i < static_cast<int>(f.size_) - 1; i++) {
            f.data_[i] = f.data_[i + 1];
            f.ch_[i] = f.ch_[i + 1];
        }

        f.size_--;
        f.data_[k] = cur_.back();

        write_page_copy(pos_, cur_);
        write_page_copy(bpos, bro);
        write_page_copy(fpos, f);

        bool need_balance = (f.size_ < PAGE_SLOT_COUNT / 2);
        if (need_balance) {
            pos_ = fpos;
            cur_ = f;
            balance();
        }
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::balance() {
    if (cur_.fa_ == -1) {
        if (cur_.size_ == 0) {
            root_ = 0;
        }

        if (cur_.type_ == PageType::Internal && cur_.size_ == 1) {
            diskpos_t child = cur_.ch_[0];
            PAGE_TYPE son;
            read_page_copy(child, son);
            son.fa_ = -1;
            write_page_copy(child, son);
            root_ = child;
        }

        return;
    }

    if (borrowl()) {
        return;
    }
    if (borrowr()) {
        return;
    }
    merge();
}

} // namespace sjtu

#endif // BPT_HPP