#pragma once
#include <vector>
#include <map>
#include <set>
#include <iostream>

using namespace std;

class CompressedRequestTree
{
public:
    static std::unique_ptr<CompressedRequestTree> NoneTree;

    CompressedRequestTree *_parent;
    vector<CompressedRequestTree *> _children;

    vector<string> _values;
    string _axis;
    vector<int> _indexes; // this is for the indexes of the grid

    vector<double> *_results;
    vector<int> _result_size;
    vector<int> _indexes_branch_size;

    CompressedRequestTree(const string axis = "root", vector<string> values = {""});
    ~CompressedRequestTree();

    string get_axis();

    void set_result(vector<double> *results);
    void set_indexes(vector<int> indexes);
    void set_result_size(vector<int> result_size);

    CompressedRequestTree *create_child(const string axis, vector<string> values);
    void add_child(CompressedRequestTree *node);
    void set_parent(CompressedRequestTree *node);

    friend ostream &operator<<(std::ostream &os, const CompressedRequestTree &obj);

    void pprint(int level = 0);
};
