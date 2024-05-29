#include "index_tree.pb.h"
#include "CompressedRequestTree.h"
using namespace std;

void encode_child(CompressedRequestTree &tree, CompressedRequestTree *child, index_tree::Node *node, vector<int> result_size = {})
{
    cout << "SHOULD HAVE ENCODED 3 TIMES" << endl;
    cout << child->_axis << endl;
    cout << size(child->_children) << endl;
    index_tree::Node *child_node = node->add_children();

    child_node->set_axis(child->_axis);
    cout << child_node->axis() << endl;

    if (size(child->_children) == 0)
    {
        result_size.push_back(size(child->_values));
        for (int size : result_size)
        {
            child_node->add_size_result(size);
        }
    }
    if (child->_results != nullptr)
    {
        for (double result : *(child->_results))
        {
            child_node->add_result(result);
        }
    }
    for (string child_val : child->_values)
    {
        child_node->add_value(child_val);
    }
    for (CompressedRequestTree *c : child->_children)
    {
        // cout << "INSIDE THE ENCODING" << endl;
        // cout << size(child->_children) << endl;
        // cout << child->_axis << endl;
        vector<int> new_result_size = result_size;
        new_result_size.push_back(size(child->_values));
        encode_child(*child, c, child_node, new_result_size);
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
    // cout << "IN ENCODE" << endl;
    // cout << size(tree._children) << endl;
    for (CompressedRequestTree *c : tree._children)
    {
        encode_child(tree, c, node);
    }
    int size = node->ByteSizeLong();
    string *array = new string[size];
    node->SerializeToString(array);
    // cout << "LOOK NOW" << endl;
    // cout << node->children_size() << endl;
    // for (int i = 0; i < node->children_size(); i++)
    // {
    //     index_tree::Node child_node = node->children(i);
    //     cout << child_node.axis() << endl;
    //     cout << "LOOK NOW X2" << endl;
    //     cout << child_node.children_size() << endl;
    //     if (child_node.axis() == "child1")
    //     {
    //         for (int j = 0; j < child_node.children_size(); j++)
    //         {
    //             index_tree::Node grandchild_node = child_node.children(j);
    //             cout << grandchild_node.axis() << endl;
    //         }
    //     }
    // }
    // cout << node->axis() << endl;
    // cout << "SIZE OF ENCODED NODE" << endl;
    // cout << node->ByteSizeLong() << endl;
    // cout << size << endl;
    return array;
}

void decode_child(index_tree::Node *node, CompressedRequestTree *tree)
{
    if (node->children_size() == 0)
    {
        cout << "INSIDE DECODE CHILD IF THERE ARE NO MORE CHILDREN" << endl;
        cout << node->axis() << endl;
        cout << tree->_axis << endl;
        for (int i = 0; i < node->result_size(); i++)
        {
            tree->_results->push_back(node->result(i));
        }
        for (int i = 0; i < node->size_result_size(); i++)
        {
            tree->_result_size.push_back(node->size_result(i));
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
    // cout << "IN DECODE" << endl;
    // cout << node->ByteSizeLong() << endl;
    // cout << node->axis() << endl;
    // cout << node->children_size() << endl;
    CompressedRequestTree *tree = new CompressedRequestTree();

    if (node->axis() != "root")
    {
        tree->_axis = node->axis();
    }
    // cout << "NOW" << endl;
    // cout << node->axis() << endl;
    // cout << node->children_size() << endl;
    decode_child(node, tree);
    cout << "INSIDE DECODE COMPLETELY" << endl;
    cout << tree->_children.size() << endl;
    for (CompressedRequestTree *child_node : tree->_children)
    {
        cout << child_node->get_axis() << endl;
    }
    return tree;
}
