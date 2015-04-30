#include "RelationalOp.h"

using namespace std;

//////////////////////   SELECT FILE OPERATION   //////////////////////

void *SetupThreadSelectFile(void *arg)
{
	((SelectFile *)arg)->StartOpSelectFile();
}

void SelectFile :: StartOpSelectFile()
{
	Record temp;
	dbfile->MoveFirst();
	while(dbfile->GetNext(temp, *myCNF, *newRec) == 1)
		out->Insert(&temp);
	out->ShutDown();
}

void SelectFile :: Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->dbfile = &inFile;
	this->out = &outPipe;
	this->myCNF = &selOp;
	this->newRec = &literal;
	pthread_create(&myThread, NULL, SetupThreadSelectFile, (void *)this);  
}

void SelectFile :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void SelectFile :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//////////////////////   SELECT PIPE OPERATION   //////////////////////
		
void *SetupThreadSelectPipe(void *arg)
{
	((SelectPipe *)arg)->StartOpSelectPipe();
}

void SelectPipe :: StartOpSelectPipe()
{
	Record temp;
	ComparisonEngine comp;
	while(in->Remove(&temp) == 1)
	{
		if(comp.Compare(&temp, newRec, myCNF) == 1)
			out->Insert(&temp);
	}
	out->ShutDown();
}

void SelectPipe :: Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->myCNF = &selOp;
	this->newRec = &literal;
	pthread_create(&myThread, NULL, SetupThreadSelectPipe, (void *)this); 
}

void SelectPipe :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void SelectPipe :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//////////////////////   PROJECT OPERATION   //////////////////////

void *SetupThreadProject(void *arg)
{
	((Project *)arg)->StartOpProject();
}

void Project :: StartOpProject()
{
	Record temp;
	while(in->Remove(&temp) == 1)
	{
		temp.Project(keepAtts, noAttsOut, noAttsIn);
		out->Insert(&temp);
	}
	in->ShutDown();
	out->ShutDown();
}

void Project ::  Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->keepAtts = keepMe;
	this->noAttsIn = numAttsInput;
	this->noAttsOut = numAttsOutput;
	pthread_create(&myThread, NULL, SetupThreadProject, (void *)this);
}

void Project :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void Project :: Use_n_Pages(int n)
{
	this->runlen = n;
}

//////////////////////   JOIN OPERATION   //////////////////////

void *SetupThreadJoin(void *arg)
{
	((Join *)arg)->StartOpJoin();
}

void Join :: StartOpJoin()
{
	int leftint, rightint;
	OrderMaker leftsortorder, rightsortorder;
	Record *recleft = new Record();
	Record *recright = new Record();
	Record *recjoin = new Record();
	ComparisonEngine comp;
	Pipe *leftPipe = new Pipe(100);
	Pipe *rightPipe = new Pipe(100);
	vector<Record *> vecLeft, vecRight;
	
	if(myCNF->GetSortOrders(leftsortorder, rightsortorder))
	{
		BigQ bigqleft(*inLeft, *leftPipe, leftsortorder, 10);
		BigQ bigqright(*inRight, *rightPipe, rightsortorder, 10);
		leftPipe->Remove(recleft);
		rightPipe->Remove(recright);
		bool leftflag, rightflag = false;
		leftint = ((int *) recleft->bits)[1] / sizeof(int) -1; 
		rightint = ((int *) recright->bits)[1] / sizeof(int) -1; 
		int attsArr[leftint + rightint];
		for(int i=0; i<leftint; i++)
			attsArr[i] = i;
		for(int i=0; i<rightint; i++)
			attsArr[leftint+i] = i;
		
		while(true)
		{
			if(comp.Compare(recleft, &leftsortorder, recright, &rightsortorder) < 0)
			{
				if(leftPipe->Remove(recleft) != 1)
				break;
			}
			else if(comp.Compare(recleft, &leftsortorder, recright, &rightsortorder) > 0)
			{
				if(rightPipe->Remove(recright) != 1)
					break;
			}
			else
			{
				leftflag = false;
				rightflag = false;
				
				while(true)
				{
					Record *dummy = new Record;
					dummy->Consume(recleft);
					vecLeft.push_back(dummy);
					if(leftPipe->Remove(recleft) != 1)
					{
						leftflag = true;
						break;
					}
					if(comp.Compare(dummy, recleft, &leftsortorder) != 0)
						break;
				}
				while(true)
				{
					Record *dummy = new Record;
					dummy->Consume(recright);
					vecRight.push_back(dummy);
					if(rightPipe->Remove(recright) != 1)
					{
						rightflag = true;
						break;
					}
					if(comp.Compare(dummy, recright, &rightsortorder) != 0)
						break;
				}
				
				for(int i = 0; i < vecLeft.size(); i++)
				{
					for(int j = 0; j < vecRight.size(); j++)
					{
						recjoin->MergeRecords(vecLeft.at(i), vecRight.at(j), leftint, rightint, attsArr, leftint+rightint, leftint);
						out->Insert(recjoin);
					}
				}
				vecLeft.clear();
				vecRight.clear();
				
				if(leftflag || rightflag)
					break;
			}
		}
	}
	else
	{
		while(inRight->Remove(recright) == 1)
		{
			Record tempRec;
			tempRec.Consume(recright);
			vecRight.push_back(&tempRec);
		}
		inLeft->Remove(recleft);
		leftint = ((int *) recleft->bits)[1] / sizeof(int) -1; 
		rightint = ((int *) vecRight.at(0)->bits)[1] / sizeof(int) -1;
		
		int attsArr[leftint+rightint];
		for(int i=0; i<leftint; i++)
			attsArr[i] = i;
		for(int i=0; i<rightint; i++)
			attsArr[leftint+i] = i;
		while(inLeft->Remove(recleft) == 1)
		{
			for(int j = 0; j < vecRight.size(); j++)
			{
				recjoin->MergeRecords(recleft, vecRight.at(j), leftint, rightint, attsArr, leftint+rightint, leftint);
				out->Insert(recjoin);
			}
		}
	}
	out->ShutDown();
}

void Join :: Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inLeft = &inPipeL;
	this->inRight = &inPipeR;
	this->out = &outPipe;
	this->myCNF = &selOp;
	this->newRec = &literal;
	pthread_create(&myThread, NULL, SetupThreadJoin, (void *)this);
}

void Join :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void Join :: Use_n_Pages(int n)
{
	this->runlen = n;
}
