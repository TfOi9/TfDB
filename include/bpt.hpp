#ifndef BPT_HPP
#define BPT_HPP

#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include "buffer.hpp"
#include "config.hpp"
#include "guard.hpp"
#include "page.hpp"
#include "log.hpp"

namespace sjtu {
#define BPT_TYPE BPlusTree<KeyType, ValueType>
#define BPT_TEMPLATE_ARGS template<typename KeyType, typename ValueType>

BPT_TEMPLATE_ARGS
class BPlusTree {
private:
    struct Context {
        std::deque<ReadGuard<KeyType, ValueType>> read_set_;
        std::deque<WriteGuard<KeyType, ValueType>> write_set_;
    };

    BUFFER_MANAGER_TYPE buffer_;
    LOGMANAGER_TYPE wal_log_;
    diskpos_t root_ = 0;
    mutable std::mutex root_latch_;

    static int clamp_index(int idx, size_t size) {
        if (size == 0) {
            return -1;
        }
        if (idx < 0) {
            return 0;
        }
        if (idx >= static_cast<int>(size)) {
            return static_cast<int>(size) - 1;
        }
        return idx;
    }

    static int find_child_index(const PAGE_TYPE &parent, diskpos_t child_pos) {
        for (int i = 0; i < static_cast<int>(parent.size_); i++) {
            if (parent.ch_[i] == child_pos) {
                return i;
            }
        }
        return -1;
    }

    diskpos_t get_root() const {
        std::lock_guard<std::mutex> lock(root_latch_);
        return root_;
    }

    void set_root(diskpos_t root) {
        std::lock_guard<std::mutex> lock(root_latch_);
        root_ = root;
        wal_log_.append_root_update(root);
    }

    bool latch_root_read(Context &ctx);
    bool latch_root_write(Context &ctx);

    diskpos_t find_leaf_read_only(const KeyType &key, Context &ctx);
    diskpos_t find_leaf_write_crabbing(const KEYPAIR_TYPE &kp, Context &ctx, bool is_insert);

    void split_upward(Context &ctx);
    void rebalance_after_erase(Context &ctx);

    void log_page(WRITE_GUARD_TYPE& guard);

public:
    BPlusTree(const std::string file_name = "bpt.dat");
    ~BPlusTree();

    std::optional<ValueType> find(const KeyType &key);
    void find_all(const KeyType &key, std::vector<ValueType> &vec);
    void insert(const KeyType &key, const ValueType &val);
    void erase(const KeyType &key, const ValueType &val);
};

BPT_TEMPLATE_ARGS
BPT_TYPE::BPlusTree(const std::string file_name) {
    if (LOGMANAGER_TYPE::needs_recovery(file_name)) {
        LOGMANAGER_TYPE::recover(file_name);
    }
    buffer_.initialise(CACHE_CAPACITY, file_name);
    wal_log_.initialise(file_name);
    root_ = buffer_.get_root_pos();
}

BPT_TEMPLATE_ARGS
BPT_TYPE::~BPlusTree() {
    buffer_.set_root_pos(get_root());
}

BPT_TEMPLATE_ARGS
bool BPT_TYPE::latch_root_read(Context &ctx) {
    ctx.read_set_.clear();
    while (true) {
        diskpos_t root_snapshot = get_root();
        if (root_snapshot == 0) {
            return false;
        }
        auto root_guard = buffer_.read_page(root_snapshot);
        if (get_root() != root_snapshot) {
            continue;
        }
        ctx.read_set_.push_back(std::move(root_guard));
        return true;
    }
}

BPT_TEMPLATE_ARGS
bool BPT_TYPE::latch_root_write(Context &ctx) {
    ctx.write_set_.clear();
    while (true) {
        diskpos_t root_snapshot = get_root();
        if (root_snapshot == 0) {
            return false;
        }
        auto root_guard = buffer_.write_page(root_snapshot);
        if (get_root() != root_snapshot) {
            continue;
        }
        ctx.write_set_.push_back(std::move(root_guard));
        return true;
    }
}

BPT_TEMPLATE_ARGS
diskpos_t BPT_TYPE::find_leaf_read_only(const KeyType &key, Context &ctx) {
    while (true) {
        auto &cur_guard = ctx.read_set_.back();
        const auto &cur = cur_guard.get_page();
        if (cur.type_ == PageType::Leaf) {
            return cur_guard.get_pos();
        }
        if (cur.size_ == 0) {
            return 0;
        }
        int k = clamp_index(cur.lower_bound(key), cur.size_);
        if (k < 0 || k >= static_cast<int>(cur.size_)) {
            return 0;
        }
        diskpos_t child = cur.ch_[k];
        auto child_guard = buffer_.read_page(child);
        ctx.read_set_.push_back(std::move(child_guard));
        if (ctx.read_set_.size() > 1) {
            ctx.read_set_.pop_front();
        }
    }
}

BPT_TEMPLATE_ARGS
diskpos_t BPT_TYPE::find_leaf_write_crabbing(const KEYPAIR_TYPE &kp, Context &ctx, bool is_insert) {
    while (true) {
        auto &cur_guard = ctx.write_set_.back();
        auto &cur = cur_guard.get_page();
        if (cur.type_ == PageType::Leaf) {
            return cur_guard.get_pos();
        }
        if (cur.size_ == 0) {
            return 0;
        }

        int k = clamp_index(cur.lower_bound(kp), cur.size_);
        if (k < 0 || k >= static_cast<int>(cur.size_)) {
            return 0;
        }

        if (is_insert && cur.data_[k] < kp) {
            cur.data_[k] = kp;
        }

        diskpos_t child = cur.ch_[k];
        auto child_guard = buffer_.write_page(child);
        ctx.write_set_.push_back(std::move(child_guard));

        if (is_insert) {
            auto &child_page = ctx.write_set_.back().get_page();
            const bool child_safe = child_page.size_ < PAGE_SLOT_COUNT - 1;
            if (child_safe) {
                while (ctx.write_set_.size() > 1) {
                    // log_page(ctx.write_set_.front());
                    ctx.write_set_.pop_front();
                }
            }
        }
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::split_upward(Context &ctx) {
    while (!ctx.write_set_.empty()) {
        auto &cur_guard = ctx.write_set_.back();
        auto &cur = cur_guard.get_page();
        if (cur.size_ < PAGE_SLOT_COUNT) {
            return;
        }

        const diskpos_t cur_pos = cur_guard.get_pos();
        const bool is_leaf = cur.type_ == PageType::Leaf;

        PAGE_TYPE newp;
        newp.size_ = PAGE_SLOT_COUNT / 2;
        cur.size_ = PAGE_SLOT_COUNT / 2;
        newp.type_ = cur.type_;
        newp.fa_ = cur.fa_;
        newp.left_ = cur_pos;
        newp.right_ = cur.right_;

        if (is_leaf) {
            for (int i = 0; i < static_cast<int>(newp.size_); i++) {
                newp.data_[i] = cur.data_[i + static_cast<int>(newp.size_)];
            }
        } else {
            for (int i = 0; i < static_cast<int>(newp.size_); i++) {
                newp.data_[i] = cur.data_[i + static_cast<int>(newp.size_)];
                newp.ch_[i] = cur.ch_[i + static_cast<int>(newp.size_)];
            }
        }

        const KEYPAIR_TYPE split_at = cur.back();
        const KEYPAIR_TYPE max_pair = newp.back();

        diskpos_t newp_pos = buffer_.insert_page(newp);
        wal_log_.append_page_update(newp_pos, newp);

        if (!is_leaf) {
            for (int i = 0; i < static_cast<int>(newp.size_); i++) {
                auto ch_guard = buffer_.write_page(newp.ch_[i]);
                ch_guard.get_page().fa_ = newp_pos;
                wal_log_.append_page_update(ch_guard.get_pos(), ch_guard.get_page());
            }
        }

        if (cur.right_ != -1) {
            auto rp_guard = buffer_.write_page(cur.right_);
            rp_guard.get_page().left_ = newp_pos;
            wal_log_.append_page_update(rp_guard.get_pos(), rp_guard.get_page());
        }
        cur.right_ = newp_pos;

        if (ctx.write_set_.size() >= 2) {
            auto &parent_guard = ctx.write_set_[ctx.write_set_.size() - 2];
            auto &parent = parent_guard.get_page();

            int fa_pos = clamp_index(parent.lower_bound(max_pair), parent.size_);
            if (fa_pos < 0) {
                fa_pos = 0;
            }

            for (int i = static_cast<int>(parent.size_) - 1; i >= fa_pos; i--) {
                parent.data_[i + 1] = parent.data_[i];
                parent.ch_[i + 1] = parent.ch_[i];
            }

            parent.data_[fa_pos] = split_at;
            parent.data_[fa_pos + 1] = max_pair;
            parent.ch_[fa_pos] = cur_pos;
            parent.ch_[fa_pos + 1] = newp_pos;
            parent.size_++;

            if (parent.size_ < PAGE_SLOT_COUNT) {
                return;
            }

            log_page(ctx.write_set_.back());
            ctx.write_set_.pop_back();
            continue;
        }

        PAGE_TYPE newr;
        newr.type_ = PageType::Internal;
        newr.size_ = 2;
        newr.data_[0] = split_at;
        newr.data_[1] = max_pair;
        newr.ch_[0] = cur_pos;
        newr.ch_[1] = newp_pos;

        diskpos_t new_root_pos = buffer_.insert_page(newr);
        wal_log_.append_page_update(new_root_pos, newr);
        cur.fa_ = new_root_pos;
        {
            auto newp_guard = buffer_.write_page(newp_pos);
            newp_guard.get_page().fa_ = new_root_pos;
            wal_log_.append_page_update(newp_guard.get_pos(), newp_guard.get_page());
        }
        set_root(new_root_pos);
        return;
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::rebalance_after_erase(Context &ctx) {
    const size_t min_size = PAGE_SLOT_COUNT / 2;

    while (!ctx.write_set_.empty()) {
        auto &cur_guard = ctx.write_set_.back();
        auto &cur = cur_guard.get_page();
        diskpos_t cur_pos = cur_guard.get_pos();

        if (ctx.write_set_.size() == 1) {
            if (cur.type_ == PageType::Leaf && cur.size_ == 0) {
                set_root(0);
                return;
            }
            if (cur.type_ == PageType::Internal && cur.size_ == 1) {
                diskpos_t child = cur.ch_[0];
                auto son_guard = buffer_.write_page(child);
                son_guard.get_page().fa_ = -1;
                wal_log_.append_page_update(son_guard.get_pos(), son_guard.get_page());
                set_root(child);
                return;
            }
            return;
        }

        if (cur.size_ >= min_size) {
            return;
        }

        auto &parent_guard = ctx.write_set_[ctx.write_set_.size() - 2];
        auto &parent = parent_guard.get_page();
        diskpos_t parent_pos = parent_guard.get_pos();
        (void)parent_pos;

        int child_idx = find_child_index(parent, cur_pos);
        if (child_idx < 0) {
            return;
        }

        const bool has_left = child_idx > 0;
        const bool has_right = child_idx + 1 < static_cast<int>(parent.size_);

        if (has_left) {
            diskpos_t left_pos = parent.ch_[child_idx - 1];
            auto left_guard = buffer_.write_page(left_pos);
            auto &left = left_guard.get_page();
            if (left.size_ > min_size) {
                if (cur.type_ == PageType::Leaf) {
                    for (int i = static_cast<int>(cur.size_) - 1; i >= 0; i--) {
                        cur.data_[i + 1] = cur.data_[i];
                        cur.ch_[i + 1] = cur.ch_[i];
                    }
                    cur.data_[0] = left.back();
                    cur.ch_[0] = left.ch_[left.size_ - 1];
                    cur.size_++;
                    left.size_--;
                    parent.data_[child_idx - 1] = left.back();
                } else {
                    for (int i = static_cast<int>(cur.size_) - 1; i >= 0; i--) {
                        cur.data_[i + 1] = cur.data_[i];
                        cur.ch_[i + 1] = cur.ch_[i];
                    }
                    cur.data_[0] = left.back();
                    cur.ch_[0] = left.ch_[left.size_ - 1];
                    cur.size_++;
                    left.size_--;

                    auto son_guard = buffer_.write_page(cur.ch_[0]);
                    son_guard.get_page().fa_ = cur_pos;
                    parent.data_[child_idx - 1] = left.back();
                    wal_log_.append_page_update(son_guard.get_pos(), son_guard.get_page());
                }
                wal_log_.append_page_update(left_guard.get_pos(), left_guard.get_page());
                return;
            }
        }

        if (has_right) {
            diskpos_t right_pos = parent.ch_[child_idx + 1];
            auto right_guard = buffer_.write_page(right_pos);
            auto &right = right_guard.get_page();
            if (right.size_ > min_size) {
                cur.data_[cur.size_] = right.data_[0];
                cur.ch_[cur.size_] = right.ch_[0];
                cur.size_++;

                for (int i = 0; i < static_cast<int>(right.size_) - 1; i++) {
                    right.data_[i] = right.data_[i + 1];
                    right.ch_[i] = right.ch_[i + 1];
                }
                right.size_--;

                if (cur.type_ == PageType::Internal) {
                    auto son_guard = buffer_.write_page(cur.ch_[cur.size_ - 1]);
                    son_guard.get_page().fa_ = cur_pos;
                    wal_log_.append_page_update(son_guard.get_pos(), son_guard.get_page());
                }
                parent.data_[child_idx] = cur.back();
                wal_log_.append_page_update(right_guard.get_pos(), right_guard.get_page());
                return;
            }
        }

        if (has_left) {
            diskpos_t left_pos = parent.ch_[child_idx - 1];
            auto left_guard = buffer_.write_page(left_pos);
            auto &left = left_guard.get_page();

            if (cur.type_ == PageType::Internal) {
                for (int i = 0; i < static_cast<int>(cur.size_); i++) {
                    auto son_guard = buffer_.write_page(cur.ch_[i]);
                    son_guard.get_page().fa_ = left_pos;
                    wal_log_.append_page_update(son_guard.get_pos(), son_guard.get_page());
                }
            }

            for (int i = 0; i < static_cast<int>(cur.size_); i++) {
                left.data_[left.size_ + i] = cur.data_[i];
                left.ch_[left.size_ + i] = cur.ch_[i];
            }
            left.size_ += cur.size_;
            cur.size_ = 0;

            left.right_ = cur.right_;
            if (cur.right_ != -1) {
                auto rp_guard = buffer_.write_page(cur.right_);
                rp_guard.get_page().left_ = left_pos;
                wal_log_.append_page_update(rp_guard.get_pos(), rp_guard.get_page());
            }

            for (int i = child_idx; i < static_cast<int>(parent.size_) - 1; i++) {
                parent.data_[i] = parent.data_[i + 1];
                parent.ch_[i] = parent.ch_[i + 1];
            }
            parent.size_--;
            if (parent.size_ > 0 && child_idx - 1 >= 0) {
                parent.data_[child_idx - 1] = left.back();
            }

            wal_log_.append_page_update(left_guard.get_pos(), left_guard.get_page());
            log_page(ctx.write_set_.back());
            ctx.write_set_.pop_back();
            continue;
        }

        if (has_right) {
            diskpos_t right_pos = parent.ch_[child_idx + 1];
            auto right_guard = buffer_.write_page(right_pos);
            auto &right = right_guard.get_page();

            if (cur.type_ == PageType::Internal) {
                for (int i = 0; i < static_cast<int>(right.size_); i++) {
                    auto son_guard = buffer_.write_page(right.ch_[i]);
                    son_guard.get_page().fa_ = cur_pos;
                    wal_log_.append_page_update(son_guard.get_pos(), son_guard.get_page());
                }
            }

            for (int i = 0; i < static_cast<int>(right.size_); i++) {
                cur.data_[cur.size_ + i] = right.data_[i];
                cur.ch_[cur.size_ + i] = right.ch_[i];
            }
            cur.size_ += right.size_;
            right.size_ = 0;

            cur.right_ = right.right_;
            if (right.right_ != -1) {
                auto rp_guard = buffer_.write_page(right.right_);
                rp_guard.get_page().left_ = cur_pos;
                wal_log_.append_page_update(rp_guard.get_pos(), rp_guard.get_page());
            }

            for (int i = child_idx + 1; i < static_cast<int>(parent.size_) - 1; i++) {
                parent.data_[i] = parent.data_[i + 1];
                parent.ch_[i] = parent.ch_[i + 1];
            }
            parent.size_--;
            if (parent.size_ > 0) {
                parent.data_[child_idx] = cur.back();
            }

            wal_log_.append_page_update(right_guard.get_pos(), right_guard.get_page());
            log_page(ctx.write_set_.back());
            ctx.write_set_.pop_back();
            continue;
        }

        return;
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::log_page(WRITE_GUARD_TYPE& guard) {
    wal_log_.append_page_update(guard.get_pos(), guard.get_page());
}

BPT_TEMPLATE_ARGS
std::optional<ValueType> BPT_TYPE::find(const KeyType &key) {
    Context ctx;
    if (!latch_root_read(ctx)) {
        return std::nullopt;
    }

    if (find_leaf_read_only(key, ctx) == 0) {
        return std::nullopt;
    }

    const auto &leaf = ctx.read_set_.back().get_page();
    int k = clamp_index(leaf.lower_bound(key), leaf.size_);
    if (k < 0 || k >= static_cast<int>(leaf.size_)) {
        return std::nullopt;
    }
    if (leaf.data_[k].key_ != key) {
        return std::nullopt;
    }
    return leaf.data_[k].val_;
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::find_all(const KeyType &key, std::vector<ValueType> &vec) {
    vec.clear();

    Context ctx;
    if (!latch_root_read(ctx)) {
        return;
    }

    if (find_leaf_read_only(key, ctx) == 0) {
        return;
    }

    int k = 0;

    while (true) {
        const auto &leaf = ctx.read_set_.back().get_page();
        const int n = static_cast<int>(leaf.size_);

        while (k < n && leaf.data_[k].key_ < key) {
            k++;
        }

        while (k < n && leaf.data_[k].key_ == key) {
            vec.push_back(leaf.data_[k].val_);
            k++;
        }

        if (k < n) {
            return;
        }
        if (leaf.right_ == -1) {
            return;
        }

        auto next_guard = buffer_.read_page(leaf.right_);
        ctx.read_set_.push_back(std::move(next_guard));
        if (ctx.read_set_.size() > 1) {
            ctx.read_set_.pop_front();
        }
        k = 0;
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::insert(const KeyType &key, const ValueType &val) {
    KEYPAIR_TYPE kp(key, val);

    while (true) {
        {
            std::lock_guard<std::mutex> lock(root_latch_);
            if (root_ == 0) {
                PAGE_TYPE newr;
                newr.type_ = PageType::Leaf;
                newr.size_ = 1;
                newr.data_[0] = kp;
                diskpos_t new_root = buffer_.insert_page(newr);
                wal_log_.append_page_update(new_root, newr);
                root_ = new_root;
                wal_log_.append_root_update(new_root);
                wal_log_.flush();
                return;
            }
        }

        Context ctx;
        if (!latch_root_write(ctx)) {
            continue;
        }

        if (find_leaf_write_crabbing(kp, ctx, true) == 0 || ctx.write_set_.empty()) {
            continue;
        }

        auto &leaf = ctx.write_set_.back().get_page();
        int k = clamp_index(leaf.lower_bound(kp), leaf.size_);

        if (leaf.size_ == 0) {
            leaf.data_[0] = kp;
            leaf.size_ = 1;

            for (auto& guard : ctx.write_set_) {
                log_page(guard);
            }
            wal_log_.flush();

            return;
        }

        if (k >= 0 && k < static_cast<int>(leaf.size_) && leaf.data_[k] == kp) {
            for (auto& guard : ctx.write_set_) {
                log_page(guard);
            }
            wal_log_.flush();
            
            return;
        }

        if (leaf.data_[k] < kp) {
            leaf.data_[k + 1] = kp;
            leaf.size_++;
        } else {
            for (int i = static_cast<int>(leaf.size_) - 1; i >= k; i--) {
                leaf.data_[i + 1] = leaf.data_[i];
            }
            leaf.data_[k] = kp;
            leaf.size_++;
        }

        if (leaf.size_ >= PAGE_SLOT_COUNT) {
            split_upward(ctx);
        }

        for (auto& guard : ctx.write_set_) {
            log_page(guard);
        }
        wal_log_.flush();

        return;
    }
}

BPT_TEMPLATE_ARGS
void BPT_TYPE::erase(const KeyType &key, const ValueType &val) {
    KEYPAIR_TYPE kp(key, val);

    Context ctx;
    if (!latch_root_write(ctx)) {
        for (auto& guard : ctx.write_set_) {
            log_page(guard);
        }
        wal_log_.flush();

        return;
    }

    if (find_leaf_write_crabbing(kp, ctx, false) == 0 || ctx.write_set_.empty()) {
        for (auto& guard : ctx.write_set_) {
            log_page(guard);
        }
        wal_log_.flush();
        
        return;
    }

    auto &leaf = ctx.write_set_.back().get_page();
    int k = clamp_index(leaf.lower_bound(kp), leaf.size_);
    if (k < 0 || k >= static_cast<int>(leaf.size_) || leaf.data_[k] != kp) {
        for (auto& guard : ctx.write_set_) {
            log_page(guard);
        }
        wal_log_.flush();
        
        return;
    }

    for (int i = k; i < static_cast<int>(leaf.size_) - 1; i++) {
        leaf.data_[i] = leaf.data_[i + 1];
    }
    leaf.size_--;

    KEYPAIR_TYPE max_pair = leaf.back();
    for (int i = static_cast<int>(ctx.write_set_.size()) - 2; i >= 0; i--) {
        auto &parent = ctx.write_set_[i].get_page();
        int p = clamp_index(parent.lower_bound(kp), parent.size_);
        if (p < 0 || p >= static_cast<int>(parent.size_)) {
            break;
        }
        if (parent.data_[p] == kp) {
            parent.data_[p] = max_pair;
        } else {
            break;
        }
    }

    if (ctx.write_set_.size() == 1) {
        if (leaf.size_ == 0) {
            set_root(0);
        }

        for (auto& guard : ctx.write_set_) {
            log_page(guard);
        }
        wal_log_.flush();
        
        return;
    }

    if (leaf.size_ < PAGE_SLOT_COUNT / 2) {
        rebalance_after_erase(ctx);
    }

    for (auto& guard : ctx.write_set_) {
        log_page(guard);
    }
    wal_log_.flush();
}

} // namespace sjtu

#endif // BPT_HPP
