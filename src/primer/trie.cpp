#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> node = root_;
  if (node == nullptr) {
    return nullptr;
  }
  for (auto &ch : key) {
    auto it = node->children_.find(ch);
    if (it == node->children_.end()) {
      return nullptr;
    }
    node = node->children_.at(ch);
  }
  if (!node->is_value_node_ || node == nullptr) {
    return nullptr;
  }
  auto value = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (value == nullptr) {
    return nullptr;
  }
  return value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  if (key.empty()) {
    std::shared_ptr<TrieNode> new_root = root_->Clone();
    std::shared_ptr<TrieNode> node =
        std::make_shared<TrieNodeWithValue<T>>(new_root->children_, std::make_shared<T>(std::move(value)));
    return Trie(node);
  }
  if (root_ == nullptr) {
    // 空树需要创建一个节点
    std::shared_ptr<const TrieNode> new_root = std::make_shared<const TrieNode>();
    const_cast<Trie *>(this)->root_ = std::move(new_root);
  }
  std::shared_ptr<TrieNode> new_root = root_->Clone();
  std::shared_ptr<TrieNode> node = new_root;
  for (size_t i = 0; i < key.size(); ++i) {
    char ch = key[i];
    if (node->children_[ch] == nullptr) {
      if (i != key.size() - 1) {
        node->children_[ch] = std::make_shared<TrieNode>();
      } else {
        node->children_[ch] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
        return Trie(new_root);
      }
    }
    auto pre_node = node;
    node = node->children_[ch]->Clone();
    if (i == key.size() - 1) {
      node = std::make_shared<TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));
      pre_node->children_[ch] = node;
      return Trie(new_root);
    }
    pre_node->children_[ch] = node;
  }
  return Trie(new_root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (key.empty()) {
    std::shared_ptr<TrieNode> new_root = root_->Clone();
    new_root->is_value_node_ = false;
    return Trie(new_root);
  }
  if (root_ == nullptr) {
    std::shared_ptr<TrieNode> new_root = root_->Clone();
    return Trie(new_root);
  }
  std::shared_ptr<TrieNode> new_root = root_->Clone();
  std::shared_ptr<TrieNode> node = new_root;
  std::vector<std::shared_ptr<TrieNode>> path;
  for (size_t i = 0; i < key.size(); ++i) {
    char ch = key[i];
    path.push_back(node);
    if (node->children_.find(ch) == node->children_.end()) {
      return Trie(new_root);
    }
    auto pre_node = node;
    node = node->children_[ch]->Clone();
    if (i == key.size() - 1) {
      node = std::make_shared<TrieNode>(node->children_);
    }
    pre_node->children_[ch] = node;
  }
  path.push_back(node);
  bool iserase = false;
  for (int i = static_cast<int>(path.size() - 1); i >= 0; i--) {
    auto tmp = path[i].get();
    if (iserase) {
      path[i]->children_.erase(key[i]);
      // std::cout<<"now is erase "<< key[i] << std::endl;
    }
    iserase = (!tmp->is_value_node_ && tmp->children_.empty());
  }
  if (path[0]->children_.empty()) {
    return Trie(nullptr);
  }
  return Trie(new_root);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub