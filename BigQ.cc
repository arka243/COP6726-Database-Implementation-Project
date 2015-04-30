#include "BigQ.h"

PQComparison :: PQComparison(OrderMaker *order)
{
        this->order=order;
}

bool PQComparison::operator ()( RunRecord* left, RunRecord* right) const
{
        ComparisonEngine engine;
        if (engine.Compare (left->record, right->record, order) >= 0) 
                return true;
        else
                return false;
}


CompareTwoRecords :: CompareTwoRecords(OrderMaker *order){
        this->order=order;
}

bool CompareTwoRecords::operator ()( Record* left, Record* right) const
{
        ComparisonEngine engine;

        if (engine.Compare (left, right, order) < 0) 
                return true;
        else
                return false;
}

void *StartOperation(void* arg) 
{
	vector<Record*> recStr;
	File file;
	Page pg;
	off_t pageIndex = 0;
	BigQStruct *bigQ = (BigQStruct *) arg;
	static int bufferCount;
	char mytempfile[100];
	RecordPQ runPQ(PQComparison(bigQ->order));
	sprintf(mytempfile,"buffer%d.tmp",bufferCount++);
	CompareTwoRecords recCompare(bigQ->order);	
	pg.EmptyItOut();
	Record temp, *rec;
	int counter = 0, popped = 0, pushed = 0;
	file.Open(0, mytempfile);
	int max_size = bigQ->runlen * PAGE_SIZE;
	int curSizeInBytes = sizeof(int);
	int recSize = 0;
	vector<int> pageIndices;
	int runCount = 0;

	while (bigQ->inPipe->Remove(&temp) == 1) 
	{
		recSize = (&temp)->GetSize(); 
		Record *newrec = new Record;
		newrec->Consume(&temp);
		if (curSizeInBytes + recSize <= max_size) 
		{
			recStr.push_back(newrec);
			pushed++;
			curSizeInBytes += recSize;
		}
		else 
		{
			runCount++;
			pageIndices.push_back(pageIndex);
			sort(recStr.begin(), recStr.end(), recCompare);
			for (int i = 0; i < recStr.size(); i++) 
			{
				rec = recStr.at(i);
				if (pg.Append(rec) == 0) 
				{
					file.AddPage(&pg, pageIndex++);
					pg.EmptyItOut();
					pg.Append(rec);
				}
				delete rec;
			}
			recStr.clear();
			if (pg.getCurSizeInBytes() > 0) 
			{
				file.AddPage(&pg, pageIndex++);
				pg.EmptyItOut();
			}
			recStr.push_back(newrec);
			curSizeInBytes = sizeof(int) + recSize;
		}
	}
    sort(recStr.begin(), recStr.end(), recCompare);
    pageIndices.push_back(pageIndex);
	for (int i = 0; i < recStr.size(); i++) 
	{
		rec = recStr.at(i);
		if (pg.Append(rec) == 0) 
		{
			file.AddPage(&pg, pageIndex++); 
			pg.EmptyItOut(); 
			pg.Append(rec);
		}
		delete rec;
	}
    recStr.clear();
    if (pg.getCurSizeInBytes() > 0) 
    {
		file.AddPage(&pg, pageIndex++);
        pg.EmptyItOut();
    }
    
    pageIndices.push_back(pageIndex);
    int numOfRuns = pageIndices.size() - 1;
    Run *runs[numOfRuns];
    for (int i = 0; i < numOfRuns; i++) 
    {
		Record* tmprec = new Record;
        runs[i] = new Run(&file, pageIndices[i], pageIndices[i + 1]);
        runs[i]->GetNext(tmprec);
        RunRecord* runRecord = new RunRecord(tmprec, runs[i]);
        runPQ.push(runRecord);
    }
    RunRecord *popPQ;
    Record* popRec;
    Run* popRun;
    while (!runPQ.empty()) 
    {
        popPQ = runPQ.top();
        runPQ.pop();
        popRun = popPQ->run;
        popRec = popPQ->record;
        bigQ->outPipe->Insert(popRec);
        delete popRec;
        Record* nextRecord = new Record;
        if (popRun->GetNext(nextRecord) == 1) 
        {
			popPQ->record = nextRecord;
			runPQ.push(popPQ);
		} 
		else 
		{
			delete popRun;
			delete popPQ;
			delete nextRecord;
		}
    }
    bigQ->outPipe->ShutDown();
    file.Close();
    remove(mytempfile);
}

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
        pthread_t myThread;
        BigQStruct *bqstruct = new BigQStruct{&in, &out, &sortorder, runlen};
        pthread_create(&myThread, NULL, StartOperation, bqstruct);

}

BigQ::~BigQ() {
}
