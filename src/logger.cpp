#include "global.h"

Logger::Logger()
{
    this->fout.open(this->logFile, ios::out);
}

void Logger::log(string logString)
{
    fout << logString << endl;
}

bool not_digit(string s){
    for(int i=0;i<s.size();i++){
        if(s[i]-'0'>9 || s[i]-'0'<0)
            return true;
    }
    return false;
}
