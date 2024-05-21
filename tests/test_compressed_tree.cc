#include <string>
#include <vector>
#include <iostream>
#include "../gribjump/tools/CompressedRequestTree.h"

using namespace std;

void test_init()
{
    string axis1 = "grandchild1";
    CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
    CompressedRequestTree *grandchild2 = new CompressedRequestTree();
    const string grandchild1_axis = (*grandchild1)._axis;
    assert((*grandchild2)._axis == "root");
    assert((*grandchild1)._axis == "grandchild1");
    assert(axis1 == "");
    delete grandchild1;
    delete grandchild2;
}

void test_add_child()
{
    string axis1 = "grandchild1";
    string axis2 = "child1";
    string axis3 = "child2";
    CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
    CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
    CompressedRequestTree *child2 = new CompressedRequestTree(axis3);
    CompressedRequestTree *root_node = new CompressedRequestTree();
    root_node->add_child(child1);
    root_node->add_child(child2);
    child1->add_child(grandchild1);
    multiset<CompressedRequestTree *> children_multiset;
    children_multiset.insert(grandchild1);
    assert((*child1)._children == children_multiset);
    delete grandchild1;
    delete child1;
    delete child2;
    delete root_node;
}

void test_create_child()
{
    string axis1 = "grandchild1";
    string axis2 = "child1";
    CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
    child1->create_child(axis1, {"0"});
    for (CompressedRequestTree *c : (*child1)._children)
    {
        cout << (*c)._axis << endl;
    }
    delete child1;
}

void test_parent()
{
    string axis1 = "grandchild1";
    string axis2 = "child1";
    CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
    CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
    child1->add_child(grandchild1);
    CompressedRequestTree *grandchild1_parent = (*grandchild1)._parent;
    cout << (*grandchild1_parent)._axis << " should be child1" << endl;
    delete grandchild1, child1;
}

void test_set_parent()
{
    string axis1 = "grandchild1";
    string axis2 = "child1";
    CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
    CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
    grandchild1->set_parent(child1);
    assert(((*(*grandchild1)._parent))._axis == "child1");
}

void test_repr()
{
    string axis1 = "child1";
    CompressedRequestTree *child1 = new CompressedRequestTree(axis1);
    child1->_values = {"0"};
    cout << *child1 << endl;
    delete child1;
}

void test_pprint()
{
    string axis1 = "child1";
    CompressedRequestTree *root = new CompressedRequestTree();
    CompressedRequestTree *child1 = new CompressedRequestTree(axis1);
    child1->_values = {"0"};
    root->add_child(child1);
    root->pprint();
    delete root, child1;
}

int main()
{
    test_init();
    test_add_child();
    test_create_child();
    test_parent();
    test_set_parent();
    test_repr();
    test_pprint();
}