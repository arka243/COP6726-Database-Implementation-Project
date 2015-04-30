#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Pipe.h"
#include <iostream>
#include "BigQ.h"
#include <cstring>

// stub DBFile header..replace it with your own DBFile.h 


class GenericDBFile {


protected:
        File file;
        Page pg;
        off_t pgIndex;
        bool wrtMode;
        char *fpath;
        bool merge;


public:

        virtual void MoveFirst ()=0;
        virtual int GetNext (Record &fetchme);
        virtual int Create(char *f_path);
        virtual int Open (char *f_path);
        virtual void Load (Schema &myschema, char *loadpath)=0;
        virtual void Add (Record &addme)=0;
        virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
        virtual int Close()=0;
        virtual ~GenericDBFile();
};

class Heap : public GenericDBFile {

public:
                Heap();
                int Create(char *f_path);
                int Open (char *f_path);
                int GetNext (Record &fetchme);
                void Load (Schema &myschema, char *loadpath);
                void Add (Record &addme);
                void MoveFirst ();
                int GetNext (Record &fetchme, CNF &cnf, Record &literal);
                int Close();
                ~Heap();

};

class Sorted : public GenericDBFile 
{
                BigQ *bigq;
                Mode mode;
                SortInfo *sortInfo;
                int buffsz;
                Pipe *input;
                Pipe *output;
                bool queryBuilt;
                bool sortOrderExists;
                OrderMaker *query;
                Page midPage;


public:

                Sorted();
                Sorted(SortInfo* info);
                int Create(char *f_path);
                int Open (char *f_path);
                void InitBigQ();
                void MoveFirst ();
                virtual int GetNext (Record &fetchme);
                void Load (Schema &myschema, char *loadpath);
                void Add (Record &addme);
                int SeqGetNext (Record &fetchme, CNF &cnf, Record &literal);
                int QGetNext(Record &fetchme, CNF &cnf, Record &literal);
                int GetNext (Record &fetchme, CNF &cnf, Record &literal);
                int BinSearch(Record& fetchme,CNF &cnf,Record &literal);
                //int QueryMaker(OrderMaker *cnfOrder, OrderMaker *sortOrder);
                int Merge();
                int Close();
                ~Sorted();
};


class DBFile {

public:
        DBFile (); 

        int Create (char *fpath, fType file_type, void *startup);
        int Open (char *fpath);
        int Close ();

        void Load (Schema &myschema, char *loadpath);

        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);

private:

        Page curPage;
        off_t curPageIndex;
        bool toWrite;
        GenericDBFile *myvar;

};
#endif
