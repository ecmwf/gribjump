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
    multiset<CompressedRequestTree *> _children;

    vector<string> _values;
    const string _axis;
    vector<int> _indexes; // this is for the indexes of the grid

    vector<double> _results;
    vector<int> _result_size;

    CompressedRequestTree(const string axis = "root", vector<string> values = {""});
    ~CompressedRequestTree();

    void set_result(vector<double> results);
    void set_indexes(vector<int> indexes);
    void set_result_size(vector<int> result_size);

    CompressedRequestTree *create_child(const string axis, vector<string> values);
    void add_child(CompressedRequestTree *node);
    void set_parent(CompressedRequestTree *node);

    vector friend struct std::hash<CompressedRequestTree>;

    //     bool operator==(const CompressedRequestTree &other) const;
    //     bool operator<(const CompressedRequestTree &other) const;
    //     friend ostream &operator<<(std::ostream &os, const CompressedRequestTree &obj);

    //     CompressedRequestTree *find_child(CompressedRequestTree *node);
    //     void pprint(int level = 0);
};

namespace std
{
    template <>
    struct hash<CompressedRequestTree>
    {
        std::size_t operator()(const CompressedRequestTree &obj) const
        {
            return std::hash<string>()((obj._axis)) ^ std::hash<vector<string>>()(obj._values);
        }
    };
}