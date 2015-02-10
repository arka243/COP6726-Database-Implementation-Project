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

// replaced with our own DBFile.cc

DBFile::DBFile()
{
	OPEN_STATUS = 0;
	pageindex = (off_t)-1;
	currec = 0;
	currpage = 0;
}


int DBFile::Create(char *filepath, fType myType, void *startup)
{
	if(filepath != NULL)
	{
		dbfile.Open(0, filepath);
		OPEN_STATUS = 1;
		return(OPEN_STATUS);
	}
	else
	{
		cout << "File not found!";
		return 0;
	}
}


int DBFile::Open(char *filepath)
{
	if(filepath != NULL)
	{
		if(OPEN_STATUS == 1)
		{
			cout << "\nFile Already Open!";
			return 0;
		}
		else
		{
			dbfile.Open(1, filepath);
			OPEN_STATUS = 1;
		}
		return(OPEN_STATUS);
	}
	else
	{
		cout << "File not found!";
		return 0;
	}
}


int DBFile::Close()
{
	if(OPEN_STATUS == 1)
		return dbfile.Close();
	else
	{
		cout << "\nFile Not Open!";
		return 0;
	}
}


void DBFile::MoveFirst()
{
	if(OPEN_STATUS == 1)
	{
		dbfile.GetPage(&currentpage, (off_t)0);
		currentrecord = currentpage.MovetoTop();
		currec = 0;
	}
	else
		cout << "\nFile not Open!";
}


void DBFile::Add(Record &addRec)
{
	if(OPEN_STATUS == 1)
	{
		Page pg;
		pageindex = dbfile.GetLength() - 1;
		dbfile.GetPage(&pg, pageindex);
		pageindex--;
		if(pg.Append(&addRec)==0)
		{
			pageindex++;
			dbfile.AddPage(&pg, pageindex);
			pg.EmptyItOut();
			pg.Append(&addRec);
		}
		pageindex++;
		dbfile.AddPage(&pg, pageindex);		
	}
	else
		cout << "\nFile Not Open!";
}


int DBFile::GetNext(Record &fetchme)
{
	if(OPEN_STATUS == 1)
	{
		if(currpage == 0 || crp.GetFirst(&fetchme) == 0)
		{
			if(currpage >= dbfile.GetLength()-1)
			{
				cout << "You've reached the end of file!";
				return 0;
			}
			dbfile.GetPage(&crp, currpage);
			currpage++;
			crp.GetFirst(&fetchme);
		}
		return 1;
	}
	else
	{
		cout << "\nFile not Open!";
		return 0;
	}
}


int DBFile::GetNext(Record &fetchRec, CNF &myCNF, Record &literal)
{
	if(OPEN_STATUS == 1)
	{
		ComparisonEngine myCompEngine;
		while(GetNext(fetchRec) == 1)
		{
			if(myCompEngine.Compare(&fetchRec,&literal, &myCNF))
				return 1;
		}
		return 0;
	}
	else
	{
		cout << "\nFile not Open";
		return 0;
	}
}


void DBFile::Load(Schema &mySchema, char *filepath)
{
	if(OPEN_STATUS == 1)
	{
		Record temp;
		Page pg;
		FILE *loadfile = fopen(filepath, "r");
		if(loadfile!=NULL)
		{
			while(temp.SuckNextRecord(&mySchema, loadfile)==1)
			{
				if(pg.Append(&temp) == 0)
				{
					pageindex++;
					dbfile.AddPage(&pg, pageindex);
					pg.EmptyItOut();
					pg.Append(&temp);
				}
			}
			pageindex++;
			dbfile.AddPage(&pg, pageindex);
		}
		else
		{
			cout << "\nError: No file found at path:" << filepath;
		}
		fclose(loadfile);
	}
	else
		cout << "\nFile not Open";
}
