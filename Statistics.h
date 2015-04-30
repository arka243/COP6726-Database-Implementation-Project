#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <vector>
#include <set>
#include <algorithm>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>

using namespace std;

struct relStruct
{
	unsigned long int TupleNum;
	int partNum;
	map <string, unsigned long int> distinctAtts;

	relStruct()
	{
		partNum = -1;
	}
};


class Statistics
{

private:
	struct attStruct
	{
		int attCount;
		long double attEstimate;
	};

	int PartitionNum;
	map <string, relStruct> relationMap;
	map <string, vector<string> > relAtts;
	map <int, vector<string> > partInfo;
	
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	int GetPartNum()
	{
		return PartitionNum;
	}

	map<string, relStruct> * GetRelMap()
	{
		return &relationMap;
	}

	map<string, vector<string> > * GetRelAtts()
	{
		return &relAtts;
	}

	map<int, vector<string> > * GetPartInfo()
	{
		return &partInfo;
	}

};


class ConverttoString
{
public:
	static string convert(int num)
	{
		stringstream sstr;
		sstr << num;
		return sstr.str();
	}
};

#endif
