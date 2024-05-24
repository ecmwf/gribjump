#include "CompressedRequestTree.h"
#include <iostream>

using namespace std;

std::unique_ptr<CompressedRequestTree> CompressedRequestTree::NoneTree = std::make_unique<CompressedRequestTree>(CompressedRequestTree("None"));

CompressedRequestTree::CompressedRequestTree(const string axis, vector<string> values)
{
    _values = values;
    _axis = axis;
    _parent = NoneTree.get();
    _results = nullptr;
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
    _children.insert(node);
    (node)->set_parent(this);
}

void CompressedRequestTree::set_parent(CompressedRequestTree *node)
{
    const string axis = (_parent)->_axis;
    if (axis != "None")
    {
        auto it = (*_parent)._children.find(this);
        multiset<CompressedRequestTree *> *parents_children = &(*_parent)._children;
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
