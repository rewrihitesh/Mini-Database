#include"semanticParser.h"

void executeCommand();

void executeALTER();
void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeGROUPBY();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeINSERT();
void executeBULKINSERT();
void executeDELETE();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);