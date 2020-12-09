#include "global.h"
/**
 * @brief 
 * SYNTAX:  ALTER TABLE <table_name> ADD|DELETE COLUMN <column_name>
 */
bool syntacticParseALTER()
{
    logger.log("syntacticParseALTER");

    if (tokenizedQuery.size() != 6 || tokenizedQuery[1] != "TABLE" 
        || (tokenizedQuery[3]!="ADD" && tokenizedQuery[3] != "DELETE" )
        || tokenizedQuery[4] != "COLUMN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ALTER;
    parsedQuery.alterRelationName = tokenizedQuery[2];
    parsedQuery.alterColumnName = tokenizedQuery[5];
    if(tokenizedQuery[3]=="ADD") parsedQuery.alterType=true; //true for add / false for delete.
    else parsedQuery.alterType=false;
    return true;
} 

bool semanticParseALTER()
{
    logger.log("semanticParseALTER");

    if (!tableCatalogue.isTable(parsedQuery.alterRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exists" << endl;
        return false;
    }

    Table table = *tableCatalogue.getTable(parsedQuery.alterRelationName);

    for(int i=0;i<table.columns.size();i++)
        if(parsedQuery.alterType != (table.columns[i]==parsedQuery.alterColumnName))
            return true;
    cout<< "SEMANTIC ERROR: Column doesn't exists in the relation or insert column is already present. "<<endl;
    return false;
}

void executeALTER(){
    logger.log("executeALTER");

    Table *table = tableCatalogue.getTable(parsedQuery.alterRelationName);
    auto columns = table->columns;
    int delcol = (!parsedQuery.alterType)?table->getColumnIndex(parsedQuery.alterColumnName):-1;
    if(parsedQuery.alterType) columns.push_back(parsedQuery.alterColumnName);
    else columns.erase(columns.begin()+delcol);
    Cursor cursor = table->getCursor();
    Table *copy = new Table(table->tableName,columns);
    for(int i=0;i<table->rowCount;i++){
        auto row= cursor.getNext();
        if(parsedQuery.alterType){
            row.push_back(0);
        }
        else row.erase(row.begin()+delcol);
        copy->writeRow<int>(row);
    }
    if(table->indexed && parsedQuery.alterColumnName != table->indexedColumn){
        copy->indexed = table->indexed;
        copy->indexedColumn = table->indexedColumn;
        copy->indexingStrategy = table->indexingStrategy;
        copy->index = table->index;
    }
    tableCatalogue.deleteTable(parsedQuery.alterRelationName);
    copy->blockify();
    if(!copy->isPermanent())
        bufferManager.deleteFile(copy->sourceFileName);
    tableCatalogue.insertTable(copy);
}