#include "global.h"

/**
 * @brief 
 * SYNTAX: DELETE FROM <table_name> VALUES <value1>[,<value2>]*
 */
bool syntacticParseDELETE()
{
    logger.log("syntacticParseDELETE");
    if (tokenizedQuery.size() < 5 || *(tokenizedQuery.begin() + 1) != "FROM" || *(tokenizedQuery.begin() + 3) != "VALUES")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = DELETE;
    parsedQuery.deleteRelationName = tokenizedQuery[2];
    for (int i = 4; i < tokenizedQuery.size(); i++){
        if(not_digit(tokenizedQuery[i])){
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.record.emplace_back(stoi(tokenizedQuery[i]));
    }
    return true;
} 

bool semanticParseDELETE()
{
    logger.log("semanticParseDELETE");

    if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exists" << endl;
        return false;
    }

    Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);

    if(table->columnCount != parsedQuery.record.size()){
        cout<<" SEMANTIC ERROR: Relation column count not equal to no. of values."<<endl;
    }

    if(table->indexed){
        vector<int> row;
        /* search in the index for the record */
        if(table->indexingStrategy == HASH){
            Linearhash *index = static_cast<Linearhash*>(table->index);
            int col = table->getColumnIndex(table->indexedColumn);
            auto records = index->get(parsedQuery.record[col]);
            for(auto record:records){
                row.clear();
                pair<int,int> p = table->rec(record);
                Page page = bufferManager.getPage(table->tableName,p.first);
                row = page.getRow(p.second);
                bool present = true;
                for(int i=0;i<row.size();i++)
                    if(row[i]!=parsedQuery.record[i])
                        present=false;
                if(present){
                    parsedQuery.rowToDelete = record;
                    return true;
                }
            }
        }
        // else
        // {
        //     /* code for b+ tree*/
        // }
    }
    else{
        /* linear search for the record to be deleted in table */
        Cursor cursor = table->getCursor();
        vector<int> row;
        for(long long int i=0;i<table->rowCount;i++){
            row.clear();
            row = cursor.getNext();
            bool present = true;  /* record present in table or not. */
            for(int j=0;j<row.size();j++)
                if(row[j] != parsedQuery.record[j])
                    present = false;
            if(present){
                parsedQuery.rowToDelete=i;
                return true;
            }
        }
    }
    cout<< "SEMANTIC ERROR: Relation doesn't contain the input valued record."<<endl;
    return false;
}

void executeDELETE(){
    logger.log("executeDELETE");

    Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);
    table->deleteRecord(parsedQuery.rowToDelete);
}