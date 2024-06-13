#include <string>
#include <vector>
#include "eckit/testing/Test.h"
#include <iostream>
using namespace eckit::testing;
#include "gribjump/tools/CompressedRequestTree.h"

using namespace std;

namespace gribjump
{
    namespace test
    {
        CASE("test_tree_init")
        {
            string axis1 = "grandchild1";
            CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
            CompressedRequestTree *grandchild2 = new CompressedRequestTree();
            const string grandchild1_axis = (*grandchild1)._axis;
            EXPECT((*grandchild2)._axis == "root");
            EXPECT((*grandchild1)._axis == "grandchild1");
            EXPECT(axis1 == "grandchild1");
            delete grandchild1;
            delete grandchild2;
        }

        CASE("test_parent")
        {
            string axis1 = "grandchild1";
            string axis2 = "child1";
            CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
            CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
            child1->add_child(grandchild1);
            CompressedRequestTree *grandchild1_parent = (*grandchild1)._parent;
            EXPECT((*grandchild1_parent)._axis == "child1");
            delete grandchild1, child1;
        }

        CASE("test_set_parent")
        {
            string axis1 = "grandchild1";
            string axis2 = "child1";
            CompressedRequestTree *grandchild1 = new CompressedRequestTree(axis1);
            CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
            grandchild1->set_parent(child1);
            EXPECT(((*(*grandchild1)._parent))._axis == "child1");
        }

        CASE("test_add_child")
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
            vector<CompressedRequestTree *> children_multiset;
            children_multiset.push_back(grandchild1);
            EXPECT((*child1)._children == children_multiset);
            delete grandchild1;
            delete child1;
            delete child2;
            delete root_node;
        }

        CASE("test_iterate_tree")
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

            auto print_path = [](typename Iterator::value_type path)
            {
                cout << "{";
                while (!path.empty())
                {
                    cout << path.top().first->_axis << ", ";
                    path.pop();
                }
                cout << "}" << endl;
            };

            for (auto &path : *root_node)
            {
                print_path(path);
            }

            delete grandchild1;
            delete child1;
            delete child2;
            delete root_node;
        }

        CASE("test_create_child")
        {
            string axis1 = "grandchild1";
            string axis2 = "child1";
            CompressedRequestTree *child1 = new CompressedRequestTree(axis2);
            child1->create_child(axis1, {"0"});
            EXPECT((*child1)._children.size() == 1);
            for (CompressedRequestTree *c : (*child1)._children)
            {
                EXPECT((*c)._axis == "grandchild1");
            }
            delete child1;
        }

        CASE("test_repr")
        {
            string axis1 = "child1";
            CompressedRequestTree *child1 = new CompressedRequestTree(axis1);
            child1->_values = {"0"};
            cout << *child1 << endl;
            delete child1;
        }

        CASE("test_pprint")
        {
            string axis1 = "child1";
            CompressedRequestTree *root = new CompressedRequestTree();
            CompressedRequestTree *child1 = new CompressedRequestTree(axis1);
            child1->_values = {"0"};
            root->add_child(child1);
            root->pprint();
            delete root, child1;
        }
    }
}

int main(int argc, char **argv)
{
    return run_tests(argc, argv);
}