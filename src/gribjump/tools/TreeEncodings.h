#include "index_tree.pb.h"
#include "CompressedRequestTree.h"
using namespace std;

void encode_child(CompressedRequestTree tree, CompressedRequestTree *child, index_tree::Node *node, vector<int> result_size = {});

string encode(CompressedRequestTree tree);

void decode_child(index_tree::Node *node, CompressedRequestTree tree);

CompressedRequestTree decode(string tree_node);
