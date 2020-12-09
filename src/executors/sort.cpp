#include"global.h"
#include"sort.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    logger.log("syntacticParseSORT");
    if(tokenizedQuery.size()!= 8 || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN"){
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    string sortingStrateg = tokenizedQuery[7];
    if(sortingStrateg == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if(sortingStrateg == "DESC")
        parsedQuery.sortingStrategy = DESC;
    else{
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    return true;
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

    if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"sort::SEMANTIC ERROR: Relation doesn't exist "<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    return true;
}
int globaColIndex=0;

bool cmp(std::vector<int> &v1, std::vector<int> &v2){
    if(parsedQuery.sortingStrategy==DESC)
        return v1[globaColIndex]>v2[globaColIndex];
    return v1[globaColIndex]<v2[globaColIndex];
}
void executeSORT(){
    logger.log("executeSORT");
    Table table = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    Table *resultantTable = new Table(parsedQuery.sortResultRelationName, table.columns);
    if(table.indexed && parsedQuery.sortColumnName==table.indexedColumn){
        if(table.indexingStrategy == HASH){
            Linearhash* hash = static_cast<Linearhash*>(table.index);
            vector<int>distinct;
            if(parsedQuery.sortingStrategy == DESC)
                distinct = hash->retrieveKeys("DESC");
            else 
                distinct = hash->retrieveKeys("ASC");
            vector<vector<int>> rows;
            int j=0;
            for(int i=0;i<distinct.size();i++){
                auto records = hash->get(distinct[i]);
                for(auto record:records){
                    if(j==table.maxRowsPerBlock){
                        resultantTable->insertRecords(rows);
                        j=0;
                        rows.clear();
                    }
                    pair<int,int>page_rec = table.rec(record);
                    Cursor cursor(table.tableName, page_rec.first);
                    rows.push_back(cursor.page.getRow(page_rec.second));
                    j++;
                }
            }
            if(j){
                resultantTable->insertRecords(rows);
                j=0;
                rows.clear();
            }
        }
        tableCatalogue.insertTable(resultantTable);
        return;
    }
    int buffer=8;
    Cursor cursor = table.getCursor();
    // vector<int> columnIndices;

    int colIndex= table.getColumnIndex(parsedQuery.sortColumnName);
    globaColIndex=colIndex;

    // when i fully understand buffer i wll add
    // max rows per block * buffer maybe??

    vector<int> row = cursor.getNext();
    int maxInMemoryRows=table.maxRowsPerBlock * buffer;
    int fileCounter=0;
    for (;!row.empty();fileCounter++){
        int tempBuf=maxInMemoryRows;
        vector<std::vector<int>> tempVector;
        while(!row.empty() and tempBuf--){
            tempVector.push_back(row);
            row = cursor.getNext();
        }
        sort(tempVector.begin(),tempVector.end(),cmp);
        // write vector to .temp
        string sourceFileName = "../data/temp/" + parsedQuery.sortResultRelationName + std::to_string(fileCounter) + ".temp";
        std::fstream fs;
        fs.open (sourceFileName, std::fstream::out | std::fstream::app);
        writeFile(tempVector,fs);
        fs.close();
    }

    ifstream srcchunks[fileCounter];
    ////////////
    if(parsedQuery.sortingStrategy==DESC){
        priority_queue<pair<int,int>> pq;
        map<int,vector<int>> mp;

        for(int i=0;i<fileCounter;i++){
            string sourceFileName = "../data/temp/" + parsedQuery.sortResultRelationName + std::to_string(i) + ".temp";
            srcchunks[i].open(sourceFileName);
            string line;
            srcchunks[i]>>line;
            vector<int> row=deflate(line);
            // for(int x:row) cout<<x<<" ";
            //     cout<<endl;
            pq.push({row[colIndex],i});
            // cout<<"Ccolindex:: "<<row[colIndex]<<endl;
            mp[i]=row;
        }

        while(!pq.empty()){
            pair<int,int> p=pq.top();
            int y=p.second;
            pq.pop();
            resultantTable->writeRow<int>(mp[y]);
            // for(int x:mp[y]) cout<<x<<" ";
            //     cout<<endl;
            string line;
            if(srcchunks[y]>>line){
                vector<int> row=deflate(line);
                pq.push({row[colIndex],y});
                // cout<<"index:: "<<row[colIndex]<<endl;
                mp[y]=row;
                // test
            }
        }

        resultantTable->blockify();
        tableCatalogue.insertTable(resultantTable);
    }
    else{
        priority_queue<pair<int,int>,vector<pair<int,int>>,greater<pair<int,int>>> pq;
        map<int,vector<int>> mp;

        for(int i=0;i<fileCounter;i++){
            string sourceFileName = "../data/temp/" + parsedQuery.sortResultRelationName + std::to_string(i) + ".temp";
            srcchunks[i].open(sourceFileName);
            string line;
            srcchunks[i]>>line;
            vector<int> row=deflate(line);
            // for(int x:row) cout<<x<<" ";
            //     cout<<endl;
            pq.push({row[colIndex],i});
            // cout<<"Ccolindex:: "<<row[colIndex]<<endl;
            mp[i]=row;
        }

        while(!pq.empty()){
            pair<int,int> p=pq.top();
            int y=p.second;
            pq.pop();
            resultantTable->writeRow<int>(mp[y]);
            // for(int x:mp[y]) cout<<x<<" ";
            //     cout<<endl;
            string line;
            if(srcchunks[y]>>line){
                vector<int> row=deflate(line);
                pq.push({row[colIndex],y});
                // cout<<"index:: "<<row[colIndex]<<endl;
                mp[y]=row;
                // test
            }
        }

        resultantTable->blockify();
        tableCatalogue.insertTable(resultantTable);
        if (!resultantTable->isPermanent())
            bufferManager.deleteFile(resultantTable->sourceFileName);
    }

    
    ///////////
    return ;
}

void writeFile(vector<std::vector<int>> file, fstream &fout)
{
    logger.log("Sort::writeFile");
    
    for(auto row:file){
        for (int columnCounter = 0; columnCounter < row.size(); columnCounter++){
            if (columnCounter != 0)
                fout << ",";
            fout << row[columnCounter];
        }
        fout << endl;
    }
}

vector<int> deflate(string line){
        // cout<<"line "<<line<<endl;
        vector<int> row;
        stringstream s(line);
        string b;
        while(getline(s,b,',')){
            int x=stoi(b);
            row.push_back(x);
        }
        return row;
}

