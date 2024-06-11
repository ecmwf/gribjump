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

        CASE("test_real_tree_decoding")
        {
            string tree_byte_array = "\n\x04root2\xd4\x02\n\x05"
                                     "class\x12\x02od*\x01\x01"
                                     "2\xc3\x02\n\x04"
                                     "date\x12\x08"
                                     "20230625*\x02\x01\x01"
                                     "2\xac\x02\n\x04time\x12\x04"
                                     "1200*\x03\x01\x01\x01"
                                     "2\x98\x02\n\x06"
                                     "domain\x12\x01g*\x04\x01\x01\x01\x01"
                                     "2\x84\x02\n\x06"
                                     "expver"
                                     "\x12\x04"
                                     "0001*\x05\x01\x01\x01\x01\x01"
                                     "2\xec\x01\n\x07levtype\x12\x03sfc*\x06\x01\x01\x01\x01\x01\x01"
                                     "2\xd3\x01\n\x05param\x12\x03"
                                     "167*\x07\x01\x01\x01\x01\x01\x01\x01"
                                     "2\xbb\x01\n\x04step\x12\x01"
                                     "0*\x08\x01\x01\x01\x01\x01\x01\x01\x01"
                                     "2\xa5\x01\n\x06stream\x12\x04oper*\t\x01\x01\x01\x01\x01\x01\x01\x01\x01"
                                     "2\x89\x01\n\x04type\x12\x02"
                                     "an\x1al\xf0\x8b\xc9\x01\xf1\x8b\xc9\x01\xf2\x8b\xc9\x01\xe4\xe3\xc8\x01\xe5\xe3\xc8\x01\xe6\xe3\xc8\x01\xdc\xbb\xc8\x01\xdd\xbb\xc8\x01\xde\xbb\xc8\x01\xf0\x8b\xc9\x01\xf1\x8b\xc9\x01\xf2\x8b\xc9\x01\xe4\xe3\xc8\x01\xe5\xe3\xc8\x01\xe6\xe3\xc8\x01\xdc\xbb\xc8\x01\xdd\xbb\xc8\x01\xde\xbb\xc8\x01\xf0\x8b\xc9\x01\xf1\x8b\xc9\x01\xf2\x8b\xc9\x01\xe4\xe3\xc8\x01\xe5\xe3\xc8\x01\xe6\xe3\xc8\x01\xdc\xbb\xc8\x01\xdd\xbb\xc8\x01\xde\xbb\xc8\x01*\n\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01:\x03\x03\x03\x03";

            string *encoded_tree = &tree_byte_array;

            CompressedRequestTree *decoded_tree = decode(encoded_tree);

            CompressedRequestTree *decoded_child1 = *(decoded_tree->_children.begin());

            decoded_child1->_parent->pprint();

            EXPECT(1 == 1);
        }
    }
}

int main(int argc, char **argv)
{
    return run_tests(argc, argv);
}