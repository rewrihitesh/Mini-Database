#include "global.h"
/**
 * @brief 
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if (tokenizedQuery.size() !=9 || tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] !="BY"
        || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupbyResultRelationName = tokenizedQuery[0];
    parsedQuery.groupbyRelationName = tokenizedQuery[6];
    parsedQuery.groupbyColumnName = tokenizedQuery[4];
    if(tokenizedQuery[8][3]!='(' || tokenizedQuery[8][tokenizedQuery[8].size()-1] !=')'){
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.groupbyaggregateType = tokenizedQuery[8].substr(0,3);
    parsedQuery.groupAggColumnName = tokenizedQuery[8].substr(4,tokenizedQuery[8].size()-5);
    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupbyResultRelationName)){
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupbyRelationName)){
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    Table table = *tableCatalogue.getTable(parsedQuery.groupbyRelationName);
    if(!table.isColumn(parsedQuery.groupbyColumnName) || !table.isColumn(parsedQuery.groupAggColumnName)){
        cout << "SEMANTIC ERROR: Column doesn't exist in the Relation table"<<endl;
    }
    
    if((parsedQuery.groupbyaggregateType == "MAX") && (parsedQuery.groupbyaggregateType == "MIN")
        &&(parsedQuery.groupbyaggregateType == "AVG") && (parsedQuery.groupbyaggregateType == "SUM")){
        cout<<" SEMANTIC ERROR: Aggregator not supported" <<endl;
    }
    return true;
}

void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    unordered_map<int,pair<int,int>> group;
    Table table = *(tableCatalogue.getTable(parsedQuery.groupbyRelationName));
    Table *resultantTable = new Table(parsedQuery.groupbyResultRelationName,
        {parsedQuery.groupbyColumnName,parsedQuery.groupbyaggregateType+parsedQuery.groupAggColumnName});
    Cursor cursor = table.getCursor();
    vector<int> row;
    int group_col = table.getColumnIndex(parsedQuery.groupbyColumnName);
    int agg_col = table.getColumnIndex(parsedQuery.groupAggColumnName);

    for(int i=0;i<table.rowCount;i++){
        row.clear();
        row = cursor.getNext();
        if(parsedQuery.groupbyaggregateType == "MAX"){
            if(group.find(row[group_col]) == group.end()){
                group[row[group_col]].first = row[agg_col];
            }
            group[row[group_col]].first = max(group[row[group_col]].first,row[agg_col]);
        }
        else if(parsedQuery.groupbyaggregateType == "MIN"){
            if(group.find(row[group_col]) == group.end()){
                group[row[group_col]].first = row[agg_col];
            }
            group[row[group_col]].first = min(group[row[group_col]].first,row[agg_col]);
        }
        else if(parsedQuery.groupbyaggregateType == "SUM")
            group[row[group_col]].first += row[agg_col];
        else if(parsedQuery.groupbyaggregateType == "AVG"){
            group[row[group_col]].first += row[agg_col];
            group[row[group_col]].second += 1;
        }
    }

    if(parsedQuery.groupbyaggregateType != "AVG"){
        for(auto &[k,v]:group)
            resultantTable->writeRow<int>({k,v.first});
    }
    else{
        for(auto &[k,v]:group)
            resultantTable->writeRow<int>({k,v.first/v.second});
    }
    resultantTable->blockify();
    if(!resultantTable->isPermanent())
        bufferManager.deleteFile(resultantTable->sourceFileName);
    tableCatalogue.insertTable(resultantTable);
}