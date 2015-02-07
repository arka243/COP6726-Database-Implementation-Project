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

// stub file... replace with your own DBFile.cc

DBFile::DBFile()
{
	OPEN_STATUS = 0;
	pageindex = (off_t)1;
	recptr = 1;
}


int DBFile::Create(char *filepath, fType myType, void *startup)
{
	dbfile.Open(0, filepath);
	OPEN_STATUS = 1;
	return(OPEN_STATUS);
}


int DBFile::Open(char *filepath)
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
	pageindex = (off_t)(-1);
	recptr = 1;
}


void DBFile::Add(Record &addRec)
{
	Page pg;
	pageindex = dbfile.GetLength();
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


int DBFile::GetNext(Record &fetchme)
{

}


int DBFile::GetNext(Record &fetchRec, CNF &myCNF, Record &literal)
{

}

void DBFile::Load(Schema &mySchema, char *filepath)
{
	Record temp;
	Page pg;
	pageindex = dbfile.GetLength();
	FILE *loadfile = fopen(filepath, "r");
	if(loadfile!=NULL)
	{
		while(temp.SuckNextRecord(&mySchema, loadfile)==1)
		{
			int append_status = pg.Append(&temp);
			if(append_status == 0)
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
