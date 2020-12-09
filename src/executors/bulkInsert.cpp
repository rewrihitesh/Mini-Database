#include "global.h"
/**
 * @brief 
 * SYNTAX:  BULK_INSERT <csv_file_name> INTO <table_name>
 */
bool syntacticParseBULKINSERT()
{
    logger.log("syntacticParseBULKINSERT");
    if (tokenizedQuery.size() !=4 || *(tokenizedQuery.begin() + 2) != "INTO")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = BULKINSERT;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    parsedQuery.insertRelationName = tokenizedQuery[3];
    return true;
} 

bool semanticParseBULKINSERT()
{
    logger.log("semanticParseBULKINSERT");

    if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exists" << endl;
        return false;
    }

    Table table = *tableCatalogue.getTable(parsedQuery.insertRelationName);
    Table *csv = new Table(parsedQuery.loadRelationName);
    fstream fin(csv->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (csv->extractColumnNames(line) && csv->columnCount == table.columnCount){
            for(int i=0;i<csv->columnCount;i++)
                if(csv->columns[i]!=table.columns[i])
                    goto err;
            fin.close();
            delete csv;
            return true;
        }   
    }
err:    
    fin.close();
    delete csv;
    cout<< "SEMANTIC ERROR: TABLE COLUMNS DOESN'T MATCH" <<endl;
    return false;
}

void executeBULKINSERT(){
    logger.log("executeBULKINSERT");
	Table *csv = new Table(parsedQuery.loadRelationName);
    Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    fstream fin(csv->sourceFileName, ios::in);
    string line,word;
    getline(fin,line);
    vector<vector<int>> rows;
    int pageCounter=0;
    while (getline(fin, line))
    {
        stringstream s(line);
        vector<int>row;
        for (int columnCounter = 0; columnCounter < table->columnCount; columnCounter++){
            getline(s, word, ',');
            row.push_back(stoi(word));
        }
        rows.push_back(row);
        pageCounter++;
        if (pageCounter == table->maxRowsPerBlock)
        {
            table->insertRecords(rows);
            pageCounter = 0;
        }
    }
    if (pageCounter)
    {
        table->insertRecords(rows);
        pageCounter = 0;
    }
    fin.close();
}