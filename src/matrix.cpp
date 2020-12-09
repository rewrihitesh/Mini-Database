 #include "matrix.h"
 using namespace std;
 Matrix::Matrix(string matrixName)
{
    // logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

bool Matrix::load(){
    // logger.log("Matrix::load");
    fstream fin(this->sourceFileName, ios::in);
    uint colcount = 0;
	char c;
	while(fin.get(c)){
		if(c=='\n')
            break;
		if(c==',')
            colcount++;
	}
    this->noOfRows = colcount+1;
    this->noOfColumns = colcount+1;
    if(this->noOfRows < this->maxRowsPerBlock){
        this->maxRowsPerBlock = this->noOfRows;
        this->maxColumnsPerBlock = this->noOfColumns;
    }
    this->rowBlockCount = ceil((double)this->noOfRows/this->maxColumnsPerBlock);// + (this->noOfRows%this->maxColumnsPerBlock)?1:0;
    this->columnBlockCount = this->rowBlockCount;
    if(this->blockify())
        return true;
    fin.close();
    return false;
}

bool Matrix::blockify(){
    // logger.log("Matrix::blockify");
    ifstream fin(this->sourceFileName);
    string word;
    pair<uint,uint> PageIndex={0,0};

    for(int i=0;i< this->noOfRows;i++){
        vector<int> rowOfPage;
        for(int j=0; j< this->noOfColumns; j++){
            word="";
            char c;
            while(fin.get(c)){
            	if(c==',' or c=='\n') break;
            	word+=c;
            }
            stringstream s(word);
            int temp;
            s>>temp;
            rowOfPage.push_back(temp);
            if(j%noOfColumns == this->maxColumnsPerBlock-1 || j == this->noOfColumns-1){
                writeRowInPage(PageIndex,rowOfPage);
                PageIndex.second++;
            }
        }

        PageIndex.second = 0;
        if(i%noOfRows == this->maxRowsPerBlock-1)
            PageIndex.first++;
    }
    return true;
}

void Matrix::writeRowInPage(pair<uint,uint> PageIndex,vector<int> &rowOfPage){

	int row=PageIndex.first;
	int col=PageIndex.second;

	string pageName="../data/temp/page_"+to_string(row)+"_"+to_string(col);

	fstream fout(pageName,ios::binary|ios::app);

	int32_t num;
	for(int i=0;i<rowOfPage.size();i++){
		num=rowOfPage[i];
		fout.write((char*) &num,sizeof(num));
	}
	fout.close();

}

vector<vector<int>> Matrix::GetPage(int r,int c){
    auto ind = getIndex(r,c);
    cout<<ind.first<<endl;
    cout<<ind.second<<endl;
    int rows= maxRowsPerBlock, cols= maxColumnsPerBlock;

    if(ind.first == rowBlockCount-1 && noOfRows%maxRowsPerBlock)
        rows = noOfRows%maxRowsPerBlock;
    if(ind.second == columnBlockCount-1 && noOfRows%maxRowsPerBlock)
        cols = noOfColumns%maxColumnsPerBlock;
    
    string pageName="../data/temp/page_"+to_string(ind.first)+"_"+to_string(ind.second);

	ifstream fin(pageName,ios::binary);

	int32_t num;

	vector<vector<int>> res(rows,std::vector<int>(cols));
	for(int i=0;i<rows and fin ;i++)
		for(int j=0;j<cols and fin;j++){
			fin.read((char*) &num, sizeof(num));
			res[i][j]=num;
			// cout<<res[i][j]<<endl;
		}
	
	fin.close();

    return res;
}

void Matrix::writeBackPage(vector<vector<int>> &matrix,int row, int col){

	string pageName="../data/temp/page_"+to_string(row)+"_"+to_string(col);

	ofstream fout(pageName,ios::binary);

	int32_t num;
	int Rows=matrix.size();
	int Columns=matrix[0].size();
	for(int i=0;i<Rows and fout ;i++)
		for(int j=0;j<Columns and fout;j++){
			num=matrix[i][j];
			fout.write((char*) &num, sizeof(num));
		}
	fout.close();
}
pair<uint,uint> Matrix::getIndex(int i, int j){
    if(this->is_transposed)
        return {j,i};
    return {i,j};
}

vector<vector<int>> Matrix::transposePage(vector<vector<int>> &matrix){

    vector<vector<int>> tmatrix(matrix.size(), vector<int>(matrix[0].size()));
    cout<<matrix.size()<<matrix[0].size()<<endl;
    for(int i=0;i<matrix.size();i++){
        for(int j=0;j<matrix[0].size();j++){
            tmatrix[i][j] = matrix[j][i];
        }
        // cout<<endl;
    }
    return tmatrix;
}
void Matrix::transpose(){
    for(int i=0;i< this->rowBlockCount;i++){
        for(int j=0;j< this->columnBlockCount;j++){
            vector<vector<int>> matrixPage = GetPage(i,j);
            matrixPage = transposePage(matrixPage);
            writeBackPage(matrixPage,i,j);
        }
    }
    this->is_transposed = !this->is_transposed;
}

// int main(){
//     Matrix A("z");
//     A.load();
//     A.transpose();
// }