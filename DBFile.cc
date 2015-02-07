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
<<<<<<< HEAD
	pageindex = (off_t)-1;
	startofpage = (off_t)0;
=======
<<<<<<< Updated upstream
=======
	pageindex = (off_t)-1;
	recptr = 1;
>>>>>>> Stashed changes
>>>>>>> origin/master
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
<<<<<<< HEAD
		if(pageindex < 0)
			pageindex++;
=======
<<<<<<< Updated upstream
		pageindex = (off_t)1;
		recptr = 1;
=======
		if(pageindex < 0)
			pageindex++;
>>>>>>> Stashed changes
>>>>>>> origin/master
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


/*void DBFile::MoveFirst()
{
	if(OPEN_STATUS == 1)
	{
<<<<<<< HEAD
		pageindex = (off_t)0;
=======
<<<<<<< Updated upstream
		pageindex = (off_t)1;
=======
		pageindex = (off_t)0;
>>>>>>> Stashed changes
>>>>>>> origin/master
		recptr = 1;
	}
	else
		cout << "\nFile not Open!";
}*/


void DBFile::MoveFirst()
{
	if(OPEN_STATUS == 1)
	{
		pageindex = (off_t)0;
		dbfile.GetPage(currentpage, pageindex);
		currentrecord = currentpage->MovetoTop();
	}
	else
		cout << "\nFile not Open!";
}


void DBFile::Add(Record &addRec)
{
	if(OPEN_STATUS == 1)
	{
		Page pg;
<<<<<<< HEAD
		pageindex = dbfile.GetLength() - 1;
=======
<<<<<<< Updated upstream
		pageindex = dbfile.GetLength();
=======
		pageindex = dbfile.GetLength() - 1;
>>>>>>> Stashed changes
>>>>>>> origin/master
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
		Page pg;
		int success = 0;
		dbfile.GetPage(&pg, startofpage);
		if(pg.GetFirst(&fetchme) == 0)
		{
			cout << "You've reached the end of the page -- Proceeding to Next Page";
			startofpage++;
			if(startofpage > dbfile.GetLength())
				cout << "You've reached the end of the file -- no more records to fetch";
			else
				dbfile.GetPage(&pg, startofpage);
		}
		else
			success = 1;	
		return(success);
	}
	else
	{
		cout << "\nFile not Open!";
		return 0;
	}
}


int DBFile::GetNext(Record &fetchRec, CNF &myCNF, Record &literal)
{
	int success = 0;
	if(OPEN_STATUS == 1)
	{
		ComparisonEngine myCompEngine;
		while(GetNext(fetchRec) == 1)
		{
			if(myCompEngine.Compare(&fetchRec, &literal, &myCNF) == 1)
			{
				success = 1;
			}	
		}
		return(success);
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
	else
		cout << "\nFile not Open";
}
