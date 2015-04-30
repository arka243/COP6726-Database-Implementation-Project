#include "Statistics.h"
#include <string.h>

using namespace std;

///////////////////// CONSTRUCTOR /////////////////////

Statistics :: Statistics()
{
	PartitionNum = 0;
}

///////////////////// COPY CONSTRUCTOR /////////////////////

Statistics :: Statistics(Statistics &copyMe)
{
	PartitionNum = copyMe.GetPartNum();
	map<string, relStruct> *copyRelMap = copyMe.GetRelMap();
	map<string, vector<string> > *copyattRel = copyMe.GetRelAtts();
	map<int, vector<string> > *copypartinfo = copyMe.GetPartInfo();
	for(map<string, relStruct>::iterator relSearch = copyRelMap->begin(); relSearch != copyRelMap->end(); relSearch++)
	{
		relStruct temprel;
		temprel.TupleNum = relSearch->second.TupleNum;
		temprel.partNum = relSearch->second.partNum;
		for(map<string, unsigned long int>::iterator distinctAttSearch = (relSearch->second).distinctAtts.begin(); distinctAttSearch != (relSearch->second).distinctAtts.end(); distinctAttSearch++)
			temprel.distinctAtts[distinctAttSearch->first] = distinctAttSearch->second;
		relationMap[relSearch->first] = temprel;
	}
	for(map<int, vector<string> >::iterator partinfoSearch = copypartinfo->begin(); partinfoSearch != copypartinfo->end(); partinfoSearch++)
	{
		vector<string> tempvec;
		vector<string> *ptrVec = &partinfoSearch->second;
		for(int i=0; i < ptrVec->size(); i++)
			tempvec.push_back(ptrVec->at(i));
		partInfo[partinfoSearch->first] = tempvec;
	}
}

///////////////////// ADDREL OPERATION /////////////////////

void Statistics :: AddRel(char *relName, int numTuples)
{
	map<string, relStruct>::iterator relSearch = relationMap.find(relName);
	if(relSearch == relationMap.end())
	{
		relStruct temprel;
		temprel.TupleNum = numTuples;
		relationMap[relName] = temprel;
	}
	else
		(relSearch->second).TupleNum = numTuples;
}

///////////////////// ADDATT OPERATION /////////////////////

void Statistics :: AddAtt(char *relName, char *attName, int numDistincts)
{
	map<string, relStruct>::iterator relSearch = relationMap.find(relName);
	if(relSearch == relationMap.end())
	{
		relStruct temprel;
		temprel.distinctAtts[attName] = numDistincts;
		relationMap[relName] = temprel;
		vector<string> tempvector;
		tempvector.push_back(relName);
		relAtts[attName] = tempvector;
	}
	else
	{
		map<string, unsigned long int>:: iterator attSearch = (relSearch->second).distinctAtts.find(attName);
		if(attSearch == (relSearch->second).distinctAtts.end())
		{
			(relSearch->second).distinctAtts[attName] = numDistincts;
			vector<string> tempvector;
			tempvector.push_back(relName);
			relAtts[attName] = tempvector;
		}
		else
			attSearch->second = numDistincts;
	}
}

///////////////////// COPYREL OPERATION /////////////////////

void Statistics :: CopyRel(char *oldName, char *newName)
{
	
	map<string, relStruct>::iterator relSearch = relationMap.find(oldName);
	relStruct *fetchRel = &(relSearch->second);
	relStruct temprel;
	temprel.TupleNum = fetchRel->TupleNum;
	for(map<string, unsigned long int>::iterator distinctAttSearch = fetchRel->distinctAtts.begin(); distinctAttSearch != fetchRel->distinctAtts.end(); distinctAttSearch++)
	{
		temprel.distinctAtts[distinctAttSearch->first] = distinctAttSearch->second;
		map<string, vector<string> >::iterator relAttsSearch = relAtts.find(distinctAttSearch->first);
		if(relAttsSearch == relAtts.end())
		{
			cout << "Error...Attribute not Found!";
			return;
		}
		(relAttsSearch->second).push_back(newName);
	}
	relationMap[newName] = temprel;
}

///////////////////// WRITE OPERATION /////////////////////

void Statistics :: Write(char *fromWhere)
{
	FILE *filepath = fopen(fromWhere, "w");
	if(filepath != NULL)
	{
		for(map<string, relStruct>::iterator relSearch = relationMap.begin(); relSearch != relationMap.end(); relSearch++)
		{
			fprintf(filepath, "\n----------");
			fprintf(filepath, "\n%s", relSearch->first.c_str());
			fprintf(filepath, "\t%lu", relSearch->second.TupleNum);
			for(map<string, unsigned long int>::iterator distinctAttSearch = (relSearch->second).distinctAtts.begin(); distinctAttSearch != (relSearch->second).distinctAtts.end(); distinctAttSearch++)
			{
				fprintf(filepath, "\n%s", distinctAttSearch->first.c_str());
				fprintf(filepath, "\t%lu", distinctAttSearch->second);
			}
			fprintf(filepath, "\n__________\n");
		}
		fclose(filepath);
	}
}

///////////////////// READ OPERATION /////////////////////

void Statistics :: Read(char *fromWhere)
{
	FILE *filepath = fopen(fromWhere, "r");
	if(filepath == NULL)
		return;
	string relname, attname;
	unsigned long int num;
	char ptr[200];
	while(fscanf(filepath, "%s", ptr) != EOF)
	{
		if(strcmp(ptr, "----------") == 0)
		{
			relStruct temprel;
			fscanf(filepath, "%s", ptr);
			relname = ptr;
			fscanf(filepath, "%lu", &temprel.TupleNum);
			fscanf(filepath, "%s", ptr);
			while(strcmp(ptr, "__________") != 0)
			{
				attname = ptr;
				fscanf(filepath, "%lu", &num);
				temprel.distinctAtts[attname] = num;
				map<string, vector<string> >::iterator relAttsSearch = relAtts.find(attname);
				if(relAttsSearch == relAtts.end())
				{
					vector<string> tempvec;
					tempvec.push_back(relname);
					relAtts[attname] = tempvec;
				}
				else
					(relAttsSearch->second).push_back(relname);
				fscanf(filepath, "%s", ptr);
			}
			relationMap[relname] = temprel;
		}
	}
}	

///////////////////// APPLY OPERATION /////////////////////

void Statistics :: Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	
	int partnNum = -1;
	int prefPos;
	string relname, relName, attName;
	set<string> relSet;
	vector<string> attsPairCombine;
	AndList *tempparseTree = parseTree;
	map<string, vector<string> >::iterator relAttsSearch;
	map<string, relStruct>::iterator relSearch;
	while(tempparseTree != NULL)
	{
		OrList* temporList = tempparseTree->left;
		while(temporList != NULL)
		{
			ComparisonOp* tempCompOp = temporList->left;
			if(tempCompOp == NULL)
				break;
			int leftnum = tempCompOp->left->code;
			string leftstr = tempCompOp->left->value;
			attsPairCombine.push_back(ConverttoString::convert(leftnum));
			attsPairCombine.push_back(leftstr);
			attsPairCombine.push_back(ConverttoString::convert(tempCompOp->code));
			int rightnum = tempCompOp->right->code;
			string rightstr = tempCompOp->right->value;
			attsPairCombine.push_back(ConverttoString::convert(rightnum));
			attsPairCombine.push_back(rightstr);
			if(leftnum == NAME)
			{
				prefPos = leftstr.find(".");
				if(prefPos != string::npos)
				{
					relName = leftstr.substr(0, prefPos);
					attName = leftstr.substr(prefPos+1);
				}
				else
				{
					attName = leftstr;
					relAttsSearch = relAtts.find(attName);
			 		if((relAttsSearch->second).size() <= 1)
						relName = (relAttsSearch->second).at(0);
				}
				relSet.insert(relName);
			}
			if(rightnum == NAME)
			{
				prefPos = rightstr.find(".");
				if(prefPos != string::npos)
				{
					relName = rightstr.substr(0, prefPos);
					attName = rightstr.substr(prefPos+1);
				}
				else
				{
					attName = rightstr;
					relAttsSearch = relAtts.find(attName);
					if((relAttsSearch->second).size() <= 1)
						relName = (relAttsSearch->second).at(0);
				}
				relSet.insert(relName);
			}
			if(temporList->rightOr != NULL)
				attsPairCombine.push_back("OR");
			temporList = temporList->rightOr;
		}
		if(tempparseTree->rightAnd != NULL)
			attsPairCombine.push_back("AND");
		attsPairCombine.push_back(".");
		tempparseTree = tempparseTree->rightAnd;
	}
	for(set<string>::iterator setSearch = relSet.begin(); setSearch != relSet.end(); setSearch++)
	{
		relSearch = relationMap.find(*setSearch);
		if(relSearch == relationMap.end())
		{
			cout<< "\nError!! Details not found";
			return;
		}
		if((relSearch->second).partNum != -1)
		{
			partnNum = (relSearch->second).partNum;
			break;
		}
	}
	double EstimateResult = Estimate(parseTree, relNames, numToJoin);
	if(partnNum == -1)
	{
		PartitionNum++;
		vector<string> vecRel;
		for(set<string>::iterator setSearch = relSet.begin(); setSearch != relSet.end(); setSearch++)
		{
			relSearch = relationMap.find(*setSearch);
			(relSearch->second).partNum = PartitionNum;
			(relSearch->second).TupleNum = (unsigned long int)EstimateResult;
			vecRel.push_back(*setSearch);
		}
		partInfo[PartitionNum] = vecRel;
	}
	else
	{
		vector<string> vecRel = partInfo[partnNum];
		for(int i=0; i < vecRel.size(); i++)
			relSet.insert(vecRel.at(i));
		vecRel.clear();
		for(set<string>::iterator setSearch = relSet.begin(); setSearch != relSet.end(); setSearch++)
		{
			relSearch = relationMap.find(*setSearch);
			(relSearch->second).partNum = partnNum;
			(relSearch->second).TupleNum = (unsigned long int)EstimateResult;
			vecRel.push_back(*setSearch);
		}
		partInfo[partnNum] = vecRel;
	}
}
					
///////////////////// ESTIMATE OPERATION /////////////////////
				
double Statistics :: Estimate(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	int prefPos;
	string last_connector = "";
	string relName, attName;
	vector<string> attsPairCombine;
    vector<long double> vecEstimate;
	set <string> relSet;
    set <string> relJoinSet;
	AndList *tempparseTree = parseTree;
	map<string, vector<string> >::iterator relAttsSearch;
	map<string, attStruct> attEstimateMap;
	for(int i=0; i < numToJoin; i++)
	{
		if(relationMap.find(relNames[i]) == relationMap.end())
		{
			cout << "\nError: Parse Tree Not Valid";
			return -1;
		}
	}
	while(tempparseTree != NULL)
	{
		OrList* temporList = tempparseTree->left;
		while(temporList != NULL)
		{
			ComparisonOp* tempCompOp = temporList->left;
			if(tempCompOp == NULL)
				break;
			int leftnum = tempCompOp->left->code;
			string leftstr = tempCompOp->left->value;
			attsPairCombine.push_back(ConverttoString::convert(leftnum));
			attsPairCombine.push_back(leftstr);
			attsPairCombine.push_back(ConverttoString::convert(tempCompOp->code));
			int rightnum = tempCompOp->right->code;
			string rightstr = tempCompOp->right->value;
			attsPairCombine.push_back(ConverttoString::convert(rightnum));
			attsPairCombine.push_back(rightstr);
			if(leftnum == NAME)
			{
				prefPos = leftstr.find(".");
				if(prefPos != string::npos)
				{
					relName = leftstr.substr(0, prefPos);
					attName = leftstr.substr(prefPos+1);
					relAttsSearch = relAtts.find(attName);
					if(relAttsSearch == relAtts.end())
					{
						cout << "\nError: Parse Tree Not Valid";
						return -1;
					}
				}
				else
				{
					attName = leftstr;
					relAttsSearch = relAtts.find(attName);
					if(relAttsSearch == relAtts.end())
					{
						cout << "\nError: Parse Tree Not Valid";
						return -1;
					}
					if((relAttsSearch->second).size() > 1)
					{
						cout << "\nError: Parse Tree Not Valid";
						return -1;
					}
					else
						relName = (relAttsSearch->second).at(0);
				}
				relSet.insert(relName);
			}
			if(rightnum == NAME)
			{
				prefPos = rightstr.find(".");
				if(prefPos != string::npos)
				{
					relName = rightstr.substr(0, prefPos);
					attName = rightstr.substr(prefPos+1);
					relAttsSearch = relAtts.find(attName);
					if(relAttsSearch == relAtts.end())
					{
						cout << "\nError: Parse Tree Not Valid";
						return -1;
					}
				}
				else
				{
					attName = rightstr;
					relAttsSearch = relAtts.find(attName);
					if(relAttsSearch == relAtts.end())
					{
						cout << "\nError: Parse Tree Not Valid";
						return -1;
					}
					if((relAttsSearch->second).size() > 1)
					{
						cout << "\nError: Parse Tree Not Valid";
						return -1;
					}
					else
						relName = (relAttsSearch->second).at(0);
				}
				relSet.insert(relName);
			}
			if(temporList->rightOr != NULL)
				attsPairCombine.push_back("OR");
			temporList = temporList->rightOr;
		}
		if(tempparseTree->rightAnd != NULL)
			attsPairCombine.push_back("AND");
		else
			attsPairCombine.push_back(".");
		tempparseTree = tempparseTree->rightAnd;
	}
	for(int i=0; i < numToJoin; i++)
	{
		relName = relNames[i];
		int partnNum = relationMap[relName].partNum;
		if(partnNum != -1)
		{
			vector<string> tempvec = partInfo[partnNum];
			for(int j=0; j < tempvec.size(); j++)
			{
				string str1 = tempvec.at(j);
				int searchreturn = 0;
				for(int k = 0; k < numToJoin; k++)
				{
					string str2 = relNames[k];
					if(str1.compare(str2) == 0)
					{
						searchreturn = 1;
						break;
					}
				}
				if(searchreturn == 0)
				{
					cout << "\nError: Parse Tree Not Valid";
					return -1;
				}
			}
		}
	}
	for(set<string>::iterator setSearch = relSet.begin(); setSearch != relSet.end(); setSearch++)
	{
		string str1 = *setSearch;
		int searchreturn = 0;
		for(int k = 0; k < numToJoin; k++)
		{
			string str2 = relNames[k];
			if(str1.compare(str2) == 0)
			{
				searchreturn = 1;
				break;
			}
		}
		if(searchreturn == 0)
		{
			cout << "\nError: Parse Tree Not Valid";
			return -1;
		}
	}
	int i = 0;
	while(i < attsPairCombine.size())
	{
		long double tempEst = -1;
		int leftAtt = atoi(attsPairCombine.at(i++).c_str());
		string Att1Val = attsPairCombine.at(i++);
		int operatorCode = atoi(attsPairCombine.at(i++).c_str());
		int rightAtt = atoi(attsPairCombine.at(i++).c_str());
		string Att2Val = attsPairCombine.at(i++);
		string connector = attsPairCombine.at(i++);
		string leftstr;
		int prefPos;
		if(leftAtt == NAME)
		{
			prefPos = Att1Val.find(".");
			if(prefPos != string::npos)
			{
				leftstr = Att1Val.substr(0, prefPos);
				Att1Val = Att1Val.substr(prefPos + 1);
			}
			else
				leftstr = relAtts[Att1Val].at(0);
			relJoinSet.insert(leftstr);
		}
		string rightstr;
		if(rightAtt == NAME)
		{
			prefPos = Att2Val.find(".");
			if(prefPos != string::npos)
			{
				rightstr = Att2Val.substr(0, prefPos);
				Att2Val = Att2Val.substr(prefPos + 1);
			}
			else
				rightstr = relAtts[Att2Val].at(0);
			relJoinSet.insert(rightstr);
		}
		if(leftAtt == NAME && rightAtt == NAME)  
		{
			relStruct t1,t2;
			t1 = relationMap[leftstr];
			t2 = relationMap[rightstr];
			tempEst = 1.0/(max(t1.distinctAtts[Att1Val], t2.distinctAtts[Att2Val]));
			vecEstimate.push_back(tempEst);
		}
		else if(leftAtt == NAME || rightAtt == NAME)
		{
			relStruct t;
			string AttName;
			if(leftAtt == NAME)
			{
				t = relationMap[leftstr];
				AttName = Att1Val;
			}
			else
			{
				t = relationMap[rightstr];
				AttName = Att2Val;
			}
			if(operatorCode == EQUALS)
			{
				if(connector.compare("OR") == 0 || last_connector.compare("OR") == 0)
				{
					if(attEstimateMap.find(AttName + "=") == attEstimateMap.end())
					{
						tempEst = (1.0- 1.0/t.distinctAtts[AttName]);
						attStruct *cce = new attStruct();
						cce->attCount = 1;
						cce->attEstimate = tempEst;
						attEstimateMap[AttName + "="] = *cce;
					}
					else
					{
						tempEst = 1.0/t.distinctAtts[AttName];
						attStruct* cce = &(attEstimateMap[AttName + "="]);
						cce->attCount += 1;
						cce->attEstimate = cce->attCount*tempEst;
					}
					if(connector.compare("OR") != 0) 
					{
						long double tempResult = 1.0;
						map<string, attStruct>::iterator attEstSearch = attEstimateMap.begin();
						for(; attEstSearch != attEstimateMap.end(); attEstSearch++)
						{
							if(attEstSearch->second.attCount == 1)
								tempResult *= attEstSearch->second.attEstimate;
							else
								tempResult *= (1 - attEstSearch->second.attEstimate);
						}
						long double totalCurrentEstimate = 1.0 - tempResult;
						vecEstimate.push_back(totalCurrentEstimate);
						attEstimateMap.clear();         
					}
				}
				else
				{
					tempEst = 1.0/t.distinctAtts[AttName];
					vecEstimate.push_back(tempEst);
				}	
			}
			else
			{
				if(connector.compare("OR") == 0 || last_connector.compare("OR") == 0)
				{
					tempEst = (1.0 - 1.0/3);
					attStruct *cce = new attStruct();
					cce->attCount = 1;
					cce->attEstimate = tempEst;
					attEstimateMap[AttName] = *cce;
					if(connector.compare("OR") != 0)  
					{
						long double tempResult = 1.0;
						map<string, attStruct>::iterator attEstSearch = attEstimateMap.begin();
						for(; attEstSearch != attEstimateMap.end(); attEstSearch++)
						{
							if(attEstSearch->second.attCount == 1)
								tempResult *= attEstSearch->second.attEstimate;
							else
								tempResult *= (1 - attEstSearch->second.attEstimate);
						}
						long double totalCurrentEstimate = 1.0 - tempResult;
						vecEstimate.push_back(totalCurrentEstimate);
						attEstimateMap.clear();
					}
				}
				else
				{
					tempEst = (1.0/3);
					vecEstimate.push_back(tempEst);
				}
			}
		}
		else    
		{
		}
		last_connector = connector;
	}
	unsigned long int it_tuples = 1;
    set <string>::iterator setSearch = relJoinSet.begin();
    for (; setSearch != relJoinSet.end(); setSearch++)
    it_tuples *= relationMap[*setSearch].TupleNum;
    double result = it_tuples;
    for(int i = 0; i < vecEstimate.size(); i++)
    {
		result *= vecEstimate.at(i);
    }
    return result;
}

///////////////////// DESTRUCTOR /////////////////////

Statistics :: ~Statistics()
{
}
