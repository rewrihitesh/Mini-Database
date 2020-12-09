#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
    this->pageName = "";
    this->tableName = "";
    this->pageIndex = -1;
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName 
 * @param pageIndex 
 */
Page::Page(string tableName, int pageIndex)
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
    Table table = *tableCatalogue.getTable(tableName);
    this->columnCount = table.columnCount;
    uint maxRowCount = table.maxRowsPerBlock;
    vector<int> row(columnCount, 0);
    this->rows.assign(maxRowCount, row);
    this->rowCount = table.rowsPerBlockCount[pageIndex];

    ifstream fin(pageName, ios::in);
    int number;
    for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
        {
            fin >> number;
            this->rows[rowCounter][columnCounter] = number;
        }
    }
    fin.close();

    // ifstream fin(pageName, ios::binary);

    // this->rowCount = table.rowsPerBlockCount[pageIndex];
    // int32_t number;
    // for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    // {   
    //     for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
    //     {
    //         fin.read((char*) & number, sizeof(number));
    //         this->rows[rowCounter][columnCounter] = (int)number;
    //     }
    // }
    // fin.close();
}

/**
 * @brief Get row from page indexed by rowIndex
 * 
 * @param rowIndex 
 * @return vector<int> 
 */
vector<int> Page::getRow(int rowIndex)
{
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    if (rowIndex >= this->rowCount)
        return result;
    return this->rows[rowIndex];
}

Page::Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/"+this->tableName + "_Page" + to_string(pageIndex);
}

/**
 * @brief writes current page contents to file.
 * 
 */
void Page::writePage()
{
    logger.log("Page::writePage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}

/**
 * @brief Appends records in a given Page.
 * 
 * @param r
 */
void Page::updatePage(vector<vector<int>>r,int pos){
    logger.log("Page::updatePage");
    /* if position is given just replace there, only happens in delete */
    if(pos>=0)
        this->rows[pos]=r[0];
    else{
        for(int i=0;i<r.size();i++){
            if(this->rows.size()==this->rowCount)
                this->rows.push_back(r[i]);
            else
                for(int j=0;j<r[i].size();j++)
                    this->rows[this->rowCount][j]=r[i][j]; 
            this->rowCount++;
        }
    }
    this->writePage();
}

/**
 * @brief removes last row from the page
 * 
 */
void Page::shrinkPage(){
    logger.log("Page::shrinkPage");
    this->rowCount--;
    this->writePage();
}
// void Page::writePage()
// {
//     logger.log("Page::writePage");

//     ofstream fout(this->pageName, ios::trunc| ios::binary);

//     int32_t num;

//     for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
//     {
//         for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
//         {

//             num = this->rows[rowCounter][columnCounter];

//             fout.write((char*) &num, sizeof(num));
//         }
//     }
//     fout.close();
// }