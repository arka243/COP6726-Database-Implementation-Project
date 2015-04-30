#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string.h>

using namespace std;

DBFile::DBFile () {
}

int DBFile::Create (char *filepath, fType f_type, void *startup) 
{
		ofstream metadata;
        char metapath[100]; 
        sprintf (metapath, "%s.meta", filepath);
        metadata.open (metapath);
        if (f_type == heap)
        {
			metadata <<"heap";
			myvar=new Heap;
        }
        else if(f_type == sorted) 
        {
			metadata <<"sorted"<<endl;
			metadata<<((SortInfo *)startup) ->runLength<<endl;
			((SortInfo*)startup)->myOrder->WriteInFileMeta(metadata);
			myvar=new Sorted((SortInfo*)startup);
        }
        else if(f_type == tree)
        {
			metadata << "tree"<<endl;
		}
		else
		{
			cout<<"File Type Not Supported";
			exit(-1);
        }
        myvar->Create(filepath);
        metadata.close();
}

int DBFile::Open (char *filepath) 
{
		ifstream metadata;
        string f_type;
        char metapath[100]; 
        sprintf (metapath, "%s.meta", filepath);
        metadata.open (metapath);
        if(!metadata.is_open())
        {
			cerr<<"Error : File Does not Exist"<<endl;
            exit(-1);
        }
        getline(metadata,f_type);
        if (f_type.compare("heap")==0)
        {
			myvar=new Heap;
        }
        else if(f_type.compare("sorted")==0) 
        {
			string runLength, whichAtt,numAtt;
			SortInfo *sortInfo=new SortInfo;
			getline(metadata,runLength);
			getline(metadata,numAtt);
			int runlen=atoi(runLength.c_str());
			int numAtts=atoi(numAtt.c_str());
			if(runlen==0 || numAtts ==0)
			{
				cout<<"Metadata file corrupted"<<endl;
				exit(-1);
			}
			sortInfo->runLength=runlen;
			int whichAtts[numAtts];
			Type whichTypes[numAtts];
			for(int i=0;i<numAtts;i++)
			{
				getline(metadata,whichAtt);
				char* att=strtok((char *)whichAtt.c_str(),"|");
				char* type=strtok(NULL,"|");
	            whichAtts[i]= atoi(att);
				if(strcmp(type,"Int")==0)
					whichTypes[i]=Int;
				else if(strcmp(type,"Double")==0)
                    whichTypes[i]=Double;
				else if(strcmp(type,"String")==0)
                    whichTypes[i]=String;
				else
				{
					cout<<"Metadata File Corrupted"<<endl;
					exit(-1);
				}
			}
			sortInfo->myOrder = new OrderMaker(numAtts, whichAtts, whichTypes);
			myvar=new Sorted(sortInfo);
        }
        else if((f_type.compare("tree")==0))
        {
			cerr << "tree";
        }
        else
        {
			cout<<"File Type not supported";
			exit(-1);
		}
		myvar->Open(filepath);
}

void DBFile::Add (Record &rec) 
{
        myvar->Add(rec);
}

void DBFile::MoveFirst () 
{
        myvar->MoveFirst();
}

int DBFile::Close () 
{
        myvar->Close();
}

int DBFile::GetNext (Record &fetchme) 
{
        myvar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
        myvar->GetNext(fetchme,cnf,literal);
}

void DBFile::Load (Schema &f_schema, char *loadpath) 
{
        myvar->Load(f_schema, loadpath);
}

int GenericDBFile::Create(char *filepath) { }

int GenericDBFile::Open (char *filepath) { }

int GenericDBFile::GetNext (Record &fetchme) { }

GenericDBFile::~GenericDBFile() { }
