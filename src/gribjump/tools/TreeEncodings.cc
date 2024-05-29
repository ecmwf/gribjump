#include "index_tree.pb.h"
#include "CompressedRequestTree.h"
using namespace std;

void encode_child(CompressedRequestTree &tree, CompressedRequestTree *child, index_tree::Node *node)
{
    index_tree::Node *child_node = node->add_children();

    child_node->set_axis(child->_axis);

    if (size(child->_children) == 0)
    {
        for (int size : child->_result_size)
        {
            child_node->add_size_result(size);
        }
        for (int index : child->_indexes)
        {
            child_node->add_indexes(index);
        }
        if (child->_results != nullptr)
        {
            for (double result : *(child->_results))
            {
                child_node->add_result(result);
            }
        }
    }
    for (string child_val : child->_values)
    {
        child_node->add_value(child_val);
    }
    for (CompressedRequestTree *c : child->_children)
    {
        encode_child(*child, c, child_node);
    }
}

string *encode(CompressedRequestTree &tree)
{
    index_tree::Node *node = new index_tree::Node;
    node->set_axis(tree._axis);
    if (tree._results != nullptr)
    {
        for (double result : *(tree._results))
        {
            node->add_result(result);
        }
    }
    for (CompressedRequestTree *c : tree._children)
    {
        encode_child(tree, c, node);
    }
    int size = node->ByteSizeLong();
    string *array = new string[size];
    node->SerializeToString(array);
    return array;
}

void decode_child(index_tree::Node *node, CompressedRequestTree *tree)
{
    if (node->children_size() == 0)
    {
        for (int i = 0; i < node->result_size(); i++)
        {
            tree->_results->push_back(node->result(i));
        }
        for (int i = 0; i < node->size_result_size(); i++)
        {
            tree->_result_size.push_back(node->size_result(i));
        }
        for (int i = 0; i < node->indexes_size(); i++)
        {
            tree->_indexes.push_back(node->indexes(i));
        }
    }
    for (int i = 0; i < node->children_size(); i++)
    {
        index_tree::Node child = node->children(i);
        string child_axis = child.axis();
        vector<string> child_vals = {};
        for (int j = 0; j < child.value_size(); j++)
        {
            child_vals.push_back(child.value(j));
        }
        CompressedRequestTree *child_node = new CompressedRequestTree(child_axis, child_vals);
        tree->add_child(child_node);
        decode_child(&child, child_node);
    }
}

CompressedRequestTree *decode(string *tree_node)
{
    index_tree::Node *node = new index_tree::Node;
    node->ParseFromString(*tree_node);
    CompressedRequestTree *tree = new CompressedRequestTree();

    if (node->axis() != "root")
    {
        tree->_axis = node->axis();
    }
    decode_child(node, tree);
    return tree;
}
