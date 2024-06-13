#include "CompressedRequestTree.h"
#include <iostream>

using namespace std;

CompressedRequestTree *Iterator::find_next_sibling(CompressedRequestTree *parent, CompressedRequestTree *child)
{
    auto it = find(parent->_children.begin(), parent->_children.end(), child);
    if (it != parent->_children.end())
    {
        ++it;
        if (it != parent->_children.end())
        {
            return *it;
        }
        else
        {
            return nullptr;
        }
    }
    return nullptr;
}

void Iterator::go_to_first_leaf_from(Path &path)
{
    while (!path.top()->_children.empty())
    {
        path.push(path.top()->_children[0]);
    }
}

Iterator &
Iterator::operator++()
{
    if (!val_.empty())
    {
        assert(val_.top()->_children.empty());
        // do something

        for (;;)
        {
            CompressedRequestTree *current_child = val_.top();
            val_.pop();
            // assert(!val_.empty());
            if (val_.empty())
            {
                // very end of iteration where we have no more leaves
                break;
            }

            CompressedRequestTree *parent = val_.top();
            CompressedRequestTree *next_sibling = find_next_sibling(parent, current_child);
            if (next_sibling != nullptr)
            {
                val_.push(next_sibling);
                go_to_first_leaf_from(val_);
                break;
            }
        }
    }

    return *this;
}

Iterator Iterator::operator++(int)
{
    This current(val_);
    ++(*this);
    return current;
}

Iterator::Iterator(CompressedRequestTree *root)
{
    val_.push(root);
    go_to_first_leaf_from(val_);
}

std::unique_ptr<CompressedRequestTree> CompressedRequestTree::NoneTree = std::make_unique<CompressedRequestTree>(CompressedRequestTree("None"));

CompressedRequestTree::CompressedRequestTree(const string axis, vector<string> values)
{
    _values = values;
    _axis = axis;
    _parent = NoneTree.get();
    _results = nullptr;
}

string CompressedRequestTree::get_axis()
{
    return _axis;
}

CompressedRequestTree::~CompressedRequestTree()
{
}

void CompressedRequestTree::set_result(vector<double> *results)
{
    _results = results;
}

void CompressedRequestTree::set_indexes(vector<int> indexes)
{
    _indexes = indexes;
}

void CompressedRequestTree::set_result_size(vector<int> result_size)
{
    _result_size = result_size;
}

CompressedRequestTree *CompressedRequestTree::create_child(const string axis, vector<string> values)
{

    CompressedRequestTree *node = new CompressedRequestTree(axis, values);
    this->add_child(node);
    return node;
}

void CompressedRequestTree::add_child(CompressedRequestTree *node)
{
    _children.push_back(node);
    (node)->set_parent(this);
}

void CompressedRequestTree::set_parent(CompressedRequestTree *node)
{
    const string axis = (_parent)->_axis;
    if (axis != "None")
    {
        auto it = find((*_parent)._children.begin(), (*_parent)._children.end(), this);
        vector<CompressedRequestTree *> *parents_children = &(*_parent)._children;
        if (it != parents_children->end())
        {
            parents_children->erase(it);
        }
    }
    _parent = node;
}

ostream &operator<<(std::ostream &os, const CompressedRequestTree &obj)
{
    if (obj._axis != "root")
    {
        os << obj._axis << "="
           << "[";
        for (int i = 0; i < size(obj._values); i++)
        {
            os << obj._values[i] << ",";
        }
        os << "]";
        return os;
    }
    else
    {
        os << obj._axis;
        return os;
    }
}

void CompressedRequestTree::pprint(int level)
{
    string tabs(level, '\t');
    cout << tabs << "\u21b3" << *this << endl;
    for (const CompressedRequestTree *child : _children)
    {
        (*const_cast<CompressedRequestTree *>(child)).pprint(level + 1);
    }
}
