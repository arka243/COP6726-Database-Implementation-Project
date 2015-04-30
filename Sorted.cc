#include "DBFile.h"

Sorted::Sorted(SortInfo *info)
{
		sortInfo = info;
        buffsz = 100;
        queryBuilt=false;
        sortOrderExists=true;
        bigq=NULL;
}

void Sorted::InitBigQ()
{
		input = new Pipe(buffsz);	
		output = new Pipe(buffsz);
		bigq=new BigQ(*input, *output,*(sortInfo->myOrder),sortInfo->runLength);
}

int Sorted::Create(char * filepath)
{
        mode=Reading;
        pgIndex=0;
        pg.EmptyItOut();
        fpath=filepath;
        file.Open(0,filepath);
        return 1;
}

int Sorted::Open(char * filepath)
{
        mode=Reading;
        pgIndex=0;
        pg.EmptyItOut();
        fpath=filepath;
        file.Open(1,filepath);
        return 1;
}

void Sorted::Add (Record &addme)
{
		if(mode==Reading)
		{
			if(!bigq)
				InitBigQ();
			mode=Writing;
		}
		input->Insert(&addme);
}

void Sorted::Load(Schema &myschema, char *loadpath)
{
		FILE *tablepath = fopen (loadpath, "r");            
		pgIndex=0;          
		Record temp;
		pg.EmptyItOut();           
		while (temp.SuckNextRecord (&myschema, tablepath) == 1)
		    Add(temp);       	
		fclose(tablepath);        
}

int Sorted::SeqGetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
        ComparisonEngine myComp;
        while(true)
        {
			if(pg.GetFirst(&fetchme)==1 )          
			{
				if (myComp.Compare (&fetchme, &literal, &cnf))    
					return 1;
			}
			else        
			{
				pgIndex++;         
				if(pgIndex<file.GetLength()-1)          
					file.GetPage(&pg,pgIndex);        
				else         
					return 0;
			}
        }
}

int Sorted :: QGetNext(Record &fetchme, CNF &cnf, Record &literal)
{
		ComparisonEngine engine;
		while(true)
		{
			if(pg.GetFirst(&fetchme)==1 )
			{         
				if(engine.Compare (&literal, query, &fetchme,sortInfo->myOrder) ==0)
				{
					if (engine.Compare (&fetchme, &literal, &cnf))    
						return 1;
				}
				else
					return 0;
			}
			else
			{              
				pgIndex++;         
				if(pgIndex<file.GetLength()-1)         
					file.GetPage(&pg,pgIndex);     
				else              
					return 0;
			}
		}
}

int Sorted::Merge()
{
		int err = 0;
		int i = 0;
		input->ShutDown();
        mode=Reading;
        if(file.GetLength()!=0)
        {
			MoveFirst();
		}
        Record *last = NULL, *prev = NULL;
        Record *fromFile=new Record;
        Record *fromPipe=new Record;
        DBFile dbfile;
        dbfile.Create("heap.tmp",heap,NULL);
        int fp=output->Remove (fromPipe);
        int ff=GetNext(*fromFile);
        ComparisonEngine engine;
        while (1) 
        {
			if (fp && ff) 
			{
				if (engine.Compare (fromPipe, fromFile, sortInfo->myOrder) >= 1) 
				{  
					dbfile.Add(*fromFile);
					ff=GetNext(*fromFile);
				}
				else
				{
					dbfile.Add(*fromPipe);
					fp=output->Remove (fromPipe);
				}
			}
			else
				break;
        }
        while(ff)
        {
			dbfile.Add(*fromFile);
			ff=GetNext(*fromFile);
        }
        while(fp)
        {
			dbfile.Add(*fromPipe);
			fp=output->Remove (fromPipe);
		}
        dbfile.Close();
        remove (fpath);
        remove("heap.tmp.md");
        rename("heap.tmp",fpath);
}

void Sorted::MoveFirst () {

        if(mode==Writing)
    		Merge();
        pgIndex=0;           
        pg.EmptyItOut();          
        if(file.GetLength()>0)
			file.GetPage(&pg,pgIndex);          
        queryBuilt=false;
        sortOrderExists=true;
}

int Sorted::GetNext (Record &fetchme)
{
		if(mode==Writing)
		{
			Merge();
		}
        if(pg.GetFirst(&fetchme)==1)                                                       
			return 1;
        else
        {
			pgIndex++;
			if(pgIndex<file.GetLength()-1)    
			{
				file.GetPage(&pg,pgIndex);     
				pg.GetFirst(&fetchme);             
				return 1;
            }
			else
				return 0;
        }
}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
		if(mode==Writing)
		{
			Merge();
			queryBuilt=false;
			sortOrderExists=true;
		}
		if(sortOrderExists)
		{
			if(!queryBuilt)
			{
				queryBuilt=true;
				int search;
				query=new OrderMaker;
				if(cnf.GetQuerySortOrders(*query,*(sortInfo->myOrder))>0)
				{  
					search=BinSearch(fetchme,cnf,literal);
					ComparisonEngine engine;
					if(search)
					{
						if (engine.Compare (&fetchme, &literal, &cnf))          
							return 1;
						else
						{
							QGetNext(fetchme, cnf, literal);
						}
					}
					else
						return 0;
				}
				else
				{
					sortOrderExists=false;
					return SeqGetNext(fetchme, cnf, literal);
				}
			}
		    else
				QGetNext(fetchme, cnf, literal);
		}
		else
			return SeqGetNext(fetchme, cnf, literal);
}

 int Sorted::BinSearch(Record& fetchme,CNF &cnf,Record &literal)
 {
		off_t first=pgIndex;
		off_t last=file.GetLength()-2;
		Record *currentRecord = new Record;
		Page *midPage=new Page;
		bool found=false; 
		ComparisonEngine engine;
		off_t mid=first;
		while(first < last)
		{
			mid=(first+last)/2;
			file.GetPage(midPage,mid);
			if(midPage->GetFirst(&fetchme)==1 )
			{      
				if (engine.Compare (&literal, query, &fetchme,sortInfo->myOrder) <= 0)
					last = mid;
				else
				{
					first=mid;
					if(first == last -1)
					{
						file.GetPage(midPage,last);
						midPage->GetFirst(&fetchme);
						if (engine.Compare (&literal, query, &fetchme,sortInfo->myOrder) > 0)
							mid=last;
						break;
					}
				}
			}
			else
				break;
		}
		if(mid==pgIndex)
		{  
			while(pg.GetFirst(&fetchme)==1)
			{
				if( engine.Compare (&literal, query, &fetchme,sortInfo->myOrder) == 0 )
				{
					found=true;
					break;
				}
			}
		}
		else
		{   
			file.GetPage(&pg,mid);
			while(pg.GetFirst(&fetchme)==1)
			{
				if( engine.Compare (&literal, query, &fetchme,sortInfo->myOrder) == 0 )
				{
					found=true;
					pgIndex=mid;
					break;
				}
			}
		}
        if(!found && mid < file.GetLength()-2)
        {
			file.GetPage(&pg,mid+1);
			if(pg.GetFirst(&fetchme)==1 && engine.Compare (&literal, query, &fetchme,sortInfo->myOrder) == 0)
			{
				found=true;
				pgIndex=mid+1;
			}
		}
		if(!found)
			return 0;
		else
			return 1;
}

int Sorted::Close()
{
		if(mode==Writing)
			Merge();
		file.Close();
}

Sorted::~Sorted()
{
		delete sortInfo->myOrder;
		delete sortInfo;
		delete query;
}

