#include "global.h"
/**
 * @brief 
 * SYNTAX: INSERT INTO <table_name> VALUES <value1>[,<value2>]*
 */
bool syntacticParseINSERT()
{
    logger.log("syntacticParseINSERT");
    if (tokenizedQuery.size() < 5 || *(tokenizedQuery.begin() + 1) != "INTO" || *(tokenizedQuery.begin() + 3) != "VALUES")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INSERT;
    parsedQuery.insertRelationName = tokenizedQuery[2];
    for (int i = 4; i < tokenizedQuery.size(); i++)
        parsedQuery.record.emplace_back(stoi(tokenizedQuery[i]));
    return true;
} 

bool semanticParseINSERT()
{
    logger.log("semanticParseINSERT");

    if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exists" << endl;
        return false;
    }

    Table table = *tableCatalogue.getTable(parsedQuery.insertRelationName);

    if(table.columnCount != parsedQuery.record.size()){
        cout<< "SEMANTIC ERROR: Relation column count not equal to no. of values.";
        return false;
    }
    return true;
}

void executeINSERT(){
    logger.log("executeINSERT");
	Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    table->insertRecords({parsedQuery.record});
}