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
		pageindex = (off_t)1;
		recptr = 1;
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
	if(OPEN_STATUS == 1)
	{
		pageindex = (off_t)1;
		recptr = 1;
	}
	else
		cout << "\nFile not Open!";
}


void DBFile::Add(Record &addRec)
{
	if(OPEN_STATUS == 1)
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
	else
		cout << "\nFile Not Open!";
}


int DBFile::GetNext(Record &fetchme)
{
	if(OPEN_STATUS == 1)
	{
		Record temprec;
		Page pg, pg_store;
		dbfile.GetPage(&pg, pageindex);
		pageindex--;
		int count = 0;
		while(count < recptr)
		{
			if(pg.GetFirst(&temprec) == 1)
			{
				fetchme = temprec;
				count++;
				pg_store.Append(&temprec);
			}
			else 
			{
				cout << "You have reached the end of the page! Proceeding to the next page";
				dbfile.AddPage(&pg_store, pageindex);
				pg_store.EmptyItOut();
				pageindex++;
				if(pageindex > dbfile.GetLength())
				{
					cout << "You have reached the end of the file!";
					pageindex--;
					return 0;
				}
				else 
				{
					count = 0;
					recptr = 1;
				}	
			}
		}
		recptr++;
		while(pg.GetFirst(&temprec) != 0)
		{
			pg.GetFirst(&temprec);
			pg_store.Append(&temprec);
		}
		pageindex++;
		dbfile.AddPage(&pg_store, pageindex);
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
		int success = 0,count = 0;
		Record temprec;
		Page pg, pg_store;
		ComparisonEngine myCompEngine;
		while(success != 1)
		{
			dbfile.GetPage(&pg, pageindex);
			pageindex--;
			while(count < recptr)
			{
				if(pg.GetFirst(&temprec) == 1)
				{
					count++;
					pg_store.Append(&temprec);
				}
				else 
				{
					cout << "You have reached the end of the page! Proceeding to the next page";
					dbfile.AddPage(&pg_store, pageindex);
					pg_store.EmptyItOut();
					pageindex++;
					if(pageindex > dbfile.GetLength())
					{
						cout << "You have reached the end of the file!";
						pageindex--;
						return 0;
					}
					else
					{
						count = 0;
						recptr = 1;
					}
				}
			}
			while(!pg.GetFirst(&temprec) != 0)
			{
				pg_store.Append(&temprec);
				recptr++;
				if(myCompEngine.Compare(&temprec, &literal, &myCNF) == 1)
				{
					success = 1;
					fetchRec = temprec;
					recptr++;
				}	
			}
			dbfile.AddPage(&pg_store, pageindex);
			pg_store.EmptyItOut();
			pageindex++;
			if(pageindex > dbfile.GetLength())
			{
				cout << "You have reached the end of the file!";
				pageindex--;
				return 0;
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
