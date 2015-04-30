#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <malloc.h>
#include <fstream>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include <string.h>
#include <sstream>

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp 
{ 

	private:
	DBFile *dbfile;
	Pipe *out;
	CNF *myCNF;
	Record *newRec;
	int runlen;
	pthread_t myThread;

	public:
	
	SelectFile(){};
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpSelectFile();
	~SelectFile(){};

};

class SelectPipe : public RelationalOp 
{
	
	private:
	Pipe *in;
	Pipe *out;
	CNF *myCNF;
	Record *newRec;
	int runlen;
	pthread_t myThread;


	public:
	SelectPipe(){};	
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpSelectPipe();
	~SelectPipe(){};
};

class Project : public RelationalOp 
{ 
	private:
	Pipe *in;
	Pipe *out;
	int *keepAtts;
	int noAttsIn;
	int noAttsOut;
	int runlen;
	pthread_t myThread;

	
	public:
	Project(){};
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpProject();
	~Project(){};
};

class WriteOut : public RelationalOp 
{
	private:
	Pipe *in;
	FILE *file;
	Schema *schema;
	int runlen;
	pthread_t myThread;

	public:
	WriteOut() {};
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpWriteOut();
	~WriteOut() {};
};

class DuplicateRemoval : public RelationalOp 
{
	private:
	Pipe *in;
	Pipe *out;
	Schema *schema;
	int runlen;
	pthread_t myThread;

	public:
	DuplicateRemoval() {};
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpDuplicateRemoval();
	~DuplicateRemoval() {};
};

class Sum : public RelationalOp 
{
	private:
	Pipe *in;
	Pipe *out;
	Function *compute;
	pthread_t myThread;
	int runlen;

	public:
	Sum() {};
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpSum();
	~Sum() {};
};

class GroupBy : public RelationalOp 
{
	private:
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	Function *compute;
	int runlen;
	pthread_t myThread;

	public:
	GroupBy() {};
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpGroupBy();
	~GroupBy() {};
};

class Join : public RelationalOp 
{
	private:
	Pipe *inLeft;
	Pipe *inRight;
	Pipe *out;
	CNF *myCNF;
	Record *newRec;
	int runlen;
	pthread_t myThread;
 
	public:
	Join() {};
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOpJoin();
	~Join() {};
};

#endif
