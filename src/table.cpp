#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName 
 */
Table::Table(string tableName)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName 
 * @param columns 
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1024) / (4 * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded 
 * @return false if an error occurred 
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word))
            return false;
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (4 * this->columnCount));
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    this->distinctValuesInColumns.assign(this->columnCount, dummy);
    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    getline(fin, line);
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            row[columnCounter] = stoi(word);
            rowsInPage[pageCounter][columnCounter] = row[columnCounter];
        }
        pageCounter++;
        this->updateStatistics(row);
        if (pageCounter == this->maxRowsPerBlock)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    if (pageCounter)
    {
        bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
    }

    if (this->rowCount == 0)
        return false;
    this->distinctValuesInColumns.clear();
    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row 
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName 
 * @return true 
 * @return false 
 */
bool Table::isColumn(string columnName)
{
    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName 
 * @param toColumnName 
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
    logger.log("Table::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    //print headings
    this->writeRow(this->columns, cout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, cout);
    }
    printRowCount(this->rowCount);
}



/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

        if (cursor->pageIndex < this->blockCount - 1)
        {
            cursor->nextPage(cursor->pageIndex+1);
        }
}



/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");
    if(!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    //print headings
    this->writeRow(this->columns, fout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, fout);
    }
    fout.close();
    this->sourceFileName = "../data/" + this->tableName + ".csv";
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
    return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload(){
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 * 
 * @return Cursor 
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 * 
 * @param columnName 
 * @return int 
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}

/**
 * @brief Function that converts rec ptr back to page id and row no.
 * 
 * @param recptr - record position in table.
 */
pair<int,int> Table::rec(int recptr){
    logger.log("Table::rec");
    return {recptr/this->maxRowsPerBlock , recptr%this->maxRowsPerBlock};
}

/**
 * @brief Function to insert key:value into index.
 * 
 * @param row in table.
 * @param recptr
 */
void Table::insertIntoIndex(vector<int> &row, int recptr){
    logger.log("Table::insertIntoIndex");
    int column = getColumnIndex(this->indexedColumn);
    /* static cast void* to hash or b+tree object according to index type */
    if(this->indexingStrategy == HASH){
        Linearhash *Index = static_cast<Linearhash*>(this->index);
        Index->insert(row[column],recptr);
    }
    // else if(this->indexingStrategy == BTREE)
    //     buildBplustree();
}
/**
 * @brief Function to create an index on specified column.
 * 
 * @param None
 */
void Table::buildIndex(){
    logger.log("Table::buildIndex");
    
    Cursor cursor(this->tableName, 0);
    vector<int> row;

    for(long long int i=0;i<this->rowCount;i++){
        row = cursor.getNext();
        this->insertIntoIndex(row,i);
    }
    // hash->print();
    this->indexed = true;
}

/** @brief Function to update key and record pointer value in 
 *          in index.
 * @param record - record vector
 * @param recordptr
 */
void Table::updateKeyfromIndex(vector<int> record, int oldrec, int recordptr){
    logger.log("Table::updateKeyfromIndex");
    if(this->indexingStrategy == HASH){
        Linearhash *Index = static_cast<Linearhash*>(this->index);
        int col = this->getColumnIndex(this->indexedColumn);
        Index->update(record[col],oldrec,recordptr);
    }
}

/** @brief Function to update key and record pointer value in 
 *          in index.
 * @param record - record vector
 * @param recordptr
 */
void Table::removeKeyfromIndex(vector<int> record, int recordptr){
    logger.log("Table::removeKeyfromIndex");
    if(this->indexingStrategy == HASH){
        Linearhash *Index = static_cast<Linearhash*>(this->index);
        int col = this->getColumnIndex(this->indexedColumn);
        Index->remove(record[col],recordptr);
    }
}

/**
 * @brief Function that inserts a row into the table relation.
 * 
 * @param row containing the elements.
 * @return yes if inserted else no.
 */
bool Table::insertRecords(vector<vector<int>> rows){
    logger.log("Table::insertRecords");
    if(rows[0].size() != this->columnCount)
        return false;
    /* insert into already free last block of table.*/
    int free = this->maxRowsPerBlock-(this->rowCount%this->maxRowsPerBlock);
    int lastPage = this->blockCount-1;
    int start=0;
    vector<vector<int>> r;
    /* do check for Lastpage existence */
    if( free != 0 && free !=this->maxRowsPerBlock && lastPage>-1){
        for(int j=0;j<free&&j<rows.size();j++){ 
            r.push_back(rows[j]);
            if(this->indexed)this->insertIntoIndex(rows[j],this->rowCount+j);
        }
        bufferManager.updatePage(this->tableName,lastPage,r,-1);
        this->rowCount+=r.size();
        this->rowsPerBlockCount[lastPage]+=r.size();
        start+=r.size();
    }
    /* if there is no space left or more rows to insert, take fresh Page. */
    while(start<rows.size()){
        r.clear();
        for(int i=start;i<start+this->maxRowsPerBlock;i++){
            if(i==rows.size())break;
            r.push_back(rows[i]);
            if(this->indexed)this->insertIntoIndex(rows[i],this->rowCount+i);
        }
        bufferManager.writePage(this->tableName, this->blockCount, r, r.size());
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(r.size());
        this->rowCount+=r.size();
        start+=r.size();
    }
    return true;
}

/** @brief Function to delete the specified row from the table.
 * 
 * @param recordptr position of the record in the table( 0 is first position).
 */
bool Table::deleteRecord(int recordptr){
    logger.log("Table::deleteRecord");
    vector<int> fillrecord, delrecord;
    int lastrecord = this->rowCount-1;
    auto lastPage = this->rec(this->rowCount-1);
    auto delRecPage =this->rec(recordptr);
    
    /*  if the record to be deleted is last in table, delete the
        last record otherwise swap it with the record to be deleted. */
    if(this->rowCount-1 != recordptr){
        fillrecord.clear();
        Cursor cursor(this->tableName,lastPage.first);
        fillrecord = cursor.page.getRow(lastPage.second);
    }
    Cursor cursor(this->tableName,delRecPage.first);
    delrecord.clear();
    delrecord = cursor.page.getRow(delRecPage.second);

    /* Last row is deleted for every delete operation */
    bufferManager.shrinkPage(this->tableName,lastPage.first);
    this->rowCount--;
    this->rowsPerBlockCount[lastPage.first]--;
    
    /* if Lastpage is empty then free it up */
    if(this->rowCount % this-> maxRowsPerBlock ==0){
        bufferManager.deleteFile(this->tableName,lastPage.first);
        this->blockCount--;
        this->rowsPerBlockCount.pop_back();
    }

    /* Replace the recordptr with last record in last page*/
    if(fillrecord.size()){
        bufferManager.updatePage(this->tableName,delRecPage.first,{fillrecord},recordptr);
        if(this->indexed)
            updateKeyfromIndex(fillrecord,lastrecord,recordptr);
    }
    if(delrecord.size() && this->indexed)
            removeKeyfromIndex(delrecord,recordptr);
    return true;
}