#ifndef BIGQ_H 
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

/*
 * Use temporary structure for passing multiple aruguments
 * to worker thread
 */
struct threadParams {
  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *sortOrder;
  int runLen;
#ifdef DEBUG
  Schema *schema;
#endif
};
typedef struct threadParams threadParams_t;

class recOnVector {
  public:
    Record *currRecord;
    int    currPageNumber;
    int    currRunNumber;



    recOnVector();
    ~recOnVector();
};

class BigQ {

public:


#ifdef DEBUG
	Schema *schema;
#endif
  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *sortOrder;
  int runLen;

#ifdef DEBUG
	BigQ (Pipe &in, 
        Pipe &out, 
        OrderMaker &sortorder, 
        int runlen,
        Schema *schema);
#else
	BigQ (Pipe &in, 
        Pipe &out, 
        OrderMaker &sortorder, 
        int runlen);
#endif
	~BigQ ();
};

#endif
