#include <string>
#include <vector>
#include "eckit/testing/Test.h"
// #include <iostream>
#include "gribjump/tools/TreeEncodings.h"

using namespace eckit::testing;
using namespace std;

namespace gribjump
{
    namespace test
    {
        CASE("test_encoding_decoding")
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

            string *encoded_tree = encode(*root_node);

            CompressedRequestTree *decoded_tree = decode(encoded_tree);

            CompressedRequestTree *decoded_child1 = *(decoded_tree->_children.begin());

            EXPECT((root_node->_children).size() == decoded_tree->_children.size());
            EXPECT(decoded_tree->_children.size() == 2);

            if (decoded_child1->get_axis() == "child1")
            {
                EXPECT((*decoded_child1)._children.size() == 1);
                CompressedRequestTree *decoded_grandchild1 = *(decoded_child1->_children.begin());
                EXPECT(decoded_grandchild1->get_axis() == "grandchild1");
            }
            else
            {
                auto iter = decoded_tree->_children.cbegin();
                advance(iter, 1);
                CompressedRequestTree *decoded_child2 = *iter;
                EXPECT(decoded_child2->get_axis() == "child1");
                CompressedRequestTree *decoded_grandchild = *(decoded_child2->_children.begin());
                EXPECT(decoded_grandchild->get_axis() == "grandchild1");
            }
            delete grandchild1;
            delete child1;
            delete child2;
            delete root_node;
        }
    }
}

int main(int argc, char **argv)
{
    return run_tests(argc, argv);
}