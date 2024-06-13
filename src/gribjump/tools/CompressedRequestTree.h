#pragma once
#include <vector>
#include <map>
#include <set>
#include <iostream>
#include <stack>
#include <iterator>
#include <utility>

using namespace std;

class CompressedRequestTree;

struct Iterator
{
    using ChildIt = typename vector<CompressedRequestTree *>::iterator;
    using Path = std::stack<pair<CompressedRequestTree *, ChildIt>>;

    using This = Iterator;

    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Path;
    using pointer = const value_type *;
    using reference = const value_type &;

    // using const_ref = const value_type &;
    // using const_ptr = const value_type *;

    // keep path in a deque instead of stack
    friend class CompressedRequestTree;

    value_type val_;

    reference operator*() const { return val_; }
    reference operator*() { return val_; }

    pointer operator->() const { return &val_; }
    pointer operator->() { return &val_; }

    This &operator=(const This &other) = default;
    // {
    //     val_ = other.val_;
    //     return *this;
    // }
    This &operator=(This &&other) = default;
    // {
    //     val_ = std::move(other.val_);
    //     return *this;
    // }

    CompressedRequestTree *find_next_sibling(CompressedRequestTree *parent, CompressedRequestTree *child);

    void go_to_first_leaf_from(Path &path);

    This &
    operator++();

    This operator++(int);

    bool operator==(const This &other) const noexcept { return val_ == other.val_; }

    bool operator!=(const This &other) const noexcept { return val_ != other.val_; }

    Iterator(CompressedRequestTree *root);

    Iterator() = default;
    Iterator(const This &) = default;
    Iterator(This &&) = default;

    Iterator(const Path &path) : val_{path} {}
};

class CompressedRequestTree
{
public:
    using iterator = Iterator;
    // TBD
    // using const_iterator = Iterator;

    iterator begin() { return Iterator{this}; }
    iterator end() { return Iterator{}; }

    static std::unique_ptr<CompressedRequestTree> NoneTree;

    CompressedRequestTree *_parent;
    vector<CompressedRequestTree *> _children;

    vector<string> _values;
    string _axis;
    vector<int> _indexes; // this is for the indexes of the grid

    vector<double> *_results;
    vector<int> _result_size;
    vector<int> _indexes_branch_size;

    CompressedRequestTree(const string axis = "root", vector<string> values = {""});
    ~CompressedRequestTree();

    string get_axis();

    void set_result(vector<double> *results);
    void set_indexes(vector<int> indexes);
    void set_result_size(vector<int> result_size);

    CompressedRequestTree *create_child(const string axis, vector<string> values);
    void add_child(CompressedRequestTree *node);
    void set_parent(CompressedRequestTree *node);

    friend ostream &operator<<(std::ostream &os, const CompressedRequestTree &obj);

    void pprint(int level = 0);
};

// struct Iterator
// {
//     using Path = std::stack<CompressedRequestTree *>;

//     using This = Iterator;

//     using iterator_category = std::forward_iterator_tag;
//     using difference_type = std::ptrdiff_t;
//     using value_type = Path;
//     using pointer = value_type *;
//     using reference = value_type &;

//     // keep path in stack
//     friend class CompressedRequestTree;
//     Iterator &operator++() noexcept;

//     value_type val_;

//     const reference operator*() const { return val_; }
//     reference operator*() { return val_; }

//     const pointer operator->() const { return &val_; }
//     pointer operator->() { return &val_; }

//     This &operator=(const This &other) = default;
//     // {
//     //     val_ = other.val_;
//     //     return *this;
//     // }
//     This &operator=(This &&other) = default;
//     // {
//     //     val_ = std::move(other.val_);
//     //     return *this;
//     // }

//     CompressedRequestTree *find_next_sibling(CompressedRequestTree *parent, CompressedRequestTree *child)
//     {
//         auto it = find(parent->_children.begin(), parent->_children.end(), child);
//         if (it != parent->_children.end())
//         {
//             ++it;
//             if (it != parent->_children.end())
//             {
//                 return *it;
//             }
//             else
//             {
//                 return nullptr;
//             }
//         }
//         return nullptr;
//     }

//     void go_to_first_leaf_from(Path &path)
//     {
//         while (!path.top()->_children.empty())
//         {
//             path.push(path.top()->_children[0]);
//         }
//     }

//     This &
//     operator++()
//     {
//         if (!val_.empty())
//         {
//             assert(val_.top()->_children.empty());
//             // do something

//             for (;;)
//             {
//                 CompressedRequestTree *current_child = val_.top();
//                 val_.pop();
//                 // assert(!val_.empty());
//                 if (!val_.empty())
//                 {
//                     // very end of iteration where we have no more leaves
//                     break;
//                 }

//                 CompressedRequestTree *parent = val_.top();
//                 CompressedRequestTree *next_sibling = find_next_sibling(parent, current_child);
//                 if (next_sibling != nullptr)
//                 {
//                     val_.push(next_sibling);
//                     go_to_first_leaf_from(val_);
//                     break;
//                 }
//             }
//         }

//         return *this;
//     }

//     This operator++(int)
//     {
//         This current(val_);
//         ++(*this);
//         return current;
//     }

//     bool operator==(const This &other) const noexcept { return val_ == other.val_; }

//     bool operator!=(const This &other) const noexcept { return val_ != other.val_; }

//     Iterator(CompressedRequestTree *root)
//     {
//         val_.push(root);
//         go_to_first_leaf_from(val_);
//     }

//     Iterator() = default;
//     Iterator(const This &) = default;
//     Iterator(This &&) = default;

//     Iterator(const Path &path) : val_{path} {}
// };
