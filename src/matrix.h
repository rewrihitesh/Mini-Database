// #include "cursor.h"
#include<bits/stdc++.h>
#include<iostream>
#include<fstream>
using namespace std;
class Matrix{
public:
    string sourceFileName = "";
    string matrixName = "";
    bool is_transposed = false;
    uint noOfRows = 0;
    uint noOfColumns = 0;
    uint maxRowsPerBlock = 40;
    uint maxColumnsPerBlock = 40;
    uint rowBlockCount = 0;
    uint columnBlockCount = 0;

    Matrix(string matrixName);
    bool blockify();
    bool load();
    void writeRowInPage(pair<uint,uint> PageIndex, vector<int> &rowOfPage);
    void transpose();
    void writeBackPage(vector<vector<int>> &matrix,int row,int col);
    pair<uint,uint> getIndex(int i, int j);
    vector<vector<int>>transposePage(vector<vector<int>> &matrix);
    vector<vector<int>> GetPage(int r, int c);
};