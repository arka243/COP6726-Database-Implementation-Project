#include "BigQ.h"
#include "vector"
#include "algorithm"
#include "exception"

OrderMaker *g_sortOrder;
int g_runCount = 0;

File g_file;
char *g_filePath = "partial.bin";

std::vector<recOnVector*> recSortVectCurrent;

std::vector<Page*> pageSortVect;

std::vector<recOnVector*> recSortVect;
std::vector<Page *> PageSortVect;
std::vector<int> pageIndexVect;
std::vector<int> pageCountPerRunVect;

recOnVector::recOnVector() 
{
	currRecord =new Record();
	currPageNumber = currRunNumber = 0;
}



void moveRunToPages(threadParams_t *inParams) 
{
  Record *currRecord = NULL;
  Page currPage;

  int pageAppendStatus = 0;
  int pageNumber = 0;

  pageNumber = g_file.GetLength();



  pageIndexVect.push_back(pageNumber);
  recOnVector *recVector;



  for (int i = 0; i < recSortVect.size(); i++) {

    currRecord = recSortVect[i]->currRecord;

    pageAppendStatus = currPage.Append(currRecord);

    if (!pageAppendStatus) {


      g_file.AddPage(&currPage, pageNumber);

      pageAppendStatus = 1;
      currPage.EmptyItOut();
      pageNumber++;

      currPage.Append(currRecord);
    }


  } 
  g_file.AddPage(&currPage, pageNumber);


  pageNumber++;
}

bool fptrSortSingleRun(const recOnVector *left, 
                  const recOnVector *right) 
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if (retVal < 0) {
    return 1;
  } else {
    return 0;
  }

}

bool mergeSortSingleRun(const recOnVector *left,const recOnVector *right) 
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if (retVal > 0) {
    return 0;
  } else {
    return 1;
  }

}

bool fptrHeapSort(const recOnVector *left,const recOnVector *right)
{
  ComparisonEngine compareengine;
  Record *record1 = (left->currRecord);
  Record *record2 = (right->currRecord);
  int compresult = compareengine.Compare(record1,record2,g_sortOrder);
  if(compresult < 0)
    return false;
  else
    return true;
}

void merge_pages(threadParams_t *inParams)
{
  recSortVect.clear();
  g_file.Open(1, g_filePath);

  Page* currPage = NULL;
  recOnVector *recVector_current;
  recOnVector *recVector_next;

  int k = 0;
  for(int i = 0; i<pageIndexVect.size(); i++)
  {
  currPage = new Page();
    g_file.GetPage(currPage,pageIndexVect[i]);
    pageSortVect.push_back(currPage);
  Record *newrecord = new Record();
    pageSortVect[i]->GetFirst(newrecord);
    recVector_current = new recOnVector();
    recVector_current->currRecord = newrecord;
    recVector_current->currRunNumber = i;
    recVector_current->currPageNumber = pageIndexVect[i];

    recSortVect.push_back(recVector_current);
  }
while(!recSortVect.empty())
  {
    int out_run = 0;
    int out_page = 0;


    std::make_heap(recSortVect.begin(),
                   recSortVect.end(),
                   fptrHeapSort);

    recVector_current = new recOnVector();
    recVector_current = recSortVect.front(); 
    out_run = recVector_current->currRunNumber;
    out_page = recVector_current->currPageNumber;
    std::pop_heap(recSortVect.begin(),recSortVect.end());
    recSortVect.pop_back();
  inParams->outPipe->Insert(recVector_current->currRecord);
    recVector_next = new recOnVector();
    recVector_next->currRunNumber = out_run;
    recVector_next->currPageNumber = out_page;
    if(pageSortVect[out_run]->GetFirst(recVector_next->currRecord)) {
      recSortVect.push_back(recVector_next);
    }
    else {
      if(out_page+2 < g_file.GetLength()){ 

        if((out_page+1)< pageIndexVect[out_run+1]) {
          g_file.GetPage(currPage,out_page+1);
          pageSortVect[out_run] = currPage;
          if(pageSortVect[out_run]->GetFirst(recVector_next->currRecord)) {
            recVector_next->currPageNumber = out_page+1;
            recSortVect.push_back(recVector_next);
          }
          currPage = new Page();
        }
      } 

    }
  }

  g_file.Close();

}

void* bigQueue(void *vptr) {
	threadParams_t *inParams = (threadParams_t *) vptr;

	Record fetchedRecord;
	Page tmpBufferPage;
	recOnVector *tmpRecordVector;

	int numPages = 0;
	bool record_present = 1;
	bool appendStatus = 0;

	g_file.Open(0, g_filePath);
	int count = 0;

	while (record_present) {
		while (numPages <= inParams->runLen) {
			if (inParams->inPipe->Remove(&fetchedRecord)) {
				tmpRecordVector = new (std::nothrow) recOnVector;
				tmpRecordVector->currRecord = new Record;

				tmpRecordVector->currRecord->Copy(&fetchedRecord);
				tmpRecordVector->currRunNumber = g_runCount;
	recSortVect.push_back(tmpRecordVector);
			
				appendStatus = tmpBufferPage.Append(&fetchedRecord);

				if (0 == appendStatus) {

					numPages++;
					tmpBufferPage.EmptyItOut();
					tmpBufferPage.Append(&fetchedRecord);
				}
			} else {
				record_present = 0;
				break;
			}
		}

		pageCountPerRunVect.push_back(numPages);

		
		numPages = 0;

		std::sort(recSortVect.begin(), 
              recSortVect.end(), 
              fptrSortSingleRun);

		moveRunToPages(inParams);
		recSortVect.clear();
		g_runCount++;
	} 

	g_file.Close();

	merge_pages(inParams);
}

#ifdef DEBUG
BigQ::BigQ(Pipe &in, 
           Pipe &out, 
           OrderMaker &sortorder, 
           int runlen,
		       Schema *schem) {
#else
BigQ::BigQ(Pipe &in, 
           Pipe &out, 
           OrderMaker &sortorder, 
           int runlen) {
#endif


	threadParams_t *tp = new (std::nothrow) threadParams_t;

#ifdef DEBUG
	tp->schema = schem;
#endif
	tp->inPipe = &in;
	tp->outPipe = &out;
	tp->sortOrder = &sortorder;
	tp->runLen = runlen;
	g_sortOrder = &sortorder;
	pthread_t thread3;
	pthread_create(&thread3, NULL, bigQueue, (void *) tp);

	pthread_join(thread3, NULL);
	out.ShutDown();
}

BigQ::~BigQ() {
}
