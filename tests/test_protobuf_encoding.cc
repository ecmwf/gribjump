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

            CompressedRequestTree decoded_tree = decode(encoded_tree);

            CompressedRequestTree *decoded_child1 = *(decoded_tree._children.begin());

            multiset<CompressedRequestTree *> children_multiset;
            children_multiset.insert(grandchild1);
            EXPECT((*decoded_child1)._children == children_multiset);
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