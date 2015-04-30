#include "DBFile.h"


Heap::Heap(){

}

int Heap::Create(char * filepath)
{
		wrtMode=true;
        pgIndex=0;
        pg.EmptyItOut();
        fpath=filepath;
        file.Open(0,filepath); 
        return 1;
}

int Heap::Open(char * filepath)
{
        wrtMode=false;
        pgIndex=0;
        pg.EmptyItOut();
        fpath=filepath;
        file.Open(1,filepath);
        return 1;
}

void Heap::Add (Record &rec) 
{
        if(pg.Append(&rec)==0)
        {                        
			file.AddPage(&pg,pgIndex++);                    
	        pg.EmptyItOut();                            
			pg.Append(&rec);                            
		}
}

void Heap::Load (Schema &f_schema, char *loadpath) 
{
        FILE *tablepath = fopen (loadpath, "r");                         
        pgIndex=0;                                
        Record temp;
        pg.EmptyItOut();                               
        while (temp.SuckNextRecord (&f_schema, tablepath) == 1) 
			Add(temp);                                     
        file.AddPage(&pg,pgIndex++);                       
        pg.EmptyItOut();                              
        fclose(tablepath);                           
}

void Heap::MoveFirst () 
{
        pgIndex=0;                                
        pg.EmptyItOut();                                
        file.GetPage(&pg,pgIndex);                        

}

int Heap::GetNext (Record &fetchme)
{
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


int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
        ComparisonEngine comp;
		while(1)
        {
			if(pg.GetFirst(&fetchme)==1 )                        
			{
				if (comp.Compare (&fetchme, &literal, &cnf))                  
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

int Heap::Close()
{
        if(wrtMode && pg.getCurSizeInBytes()>0)
        {
			file.AddPage(&pg,pgIndex++);                       
			pg.EmptyItOut();                                     
			wrtMode=false;
		}
		file.Close();                                          
		return 1;
}

Heap::~Heap(){
}

