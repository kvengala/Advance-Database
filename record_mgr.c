#include<stdio.h>
#include<stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr_stat.h"
#include "pthread.h"
#include "record_mgr.h"

//static char tmp1[1000];

// Linked list to help handle the various scan function transfer data between them
typedef struct SNode
{
    RM_ScanHandle *scanHandle;
    int page;
    int slot;
    int totalrecordlength;
    int totalrecordsinpage;
    int totalnumPages;
    BM_PageHandle *ph;
    struct SNode *nextSNode;
} sNode,*sptr;

static sptr stptr=NULL;
static Schema Globalschema;

//Functions to handle the Linked list
/*function to insert node into the global linked list startnode */
bool insert_snode(sptr *startnode , RM_ScanHandle *scanHandle , sNode *node)
{
sptr prevNode;
sptr currNode;
//newNode=(sNode *)malloc(sizeof(sNode);

if(node!=NULL)
{
	node->scanHandle=scanHandle;
	node->nextSNode=NULL;
	
	prevNode=NULL;
	currNode=*startnode;

	while(currNode!=NULL)
	{
		prevNode=currNode;
		currNode=currNode->nextSNode;
	}
	if(prevNode==NULL){
	*startnode=node; }
	else {
	prevNode->nextSNode=node; }
	
	return TRUE;
}
else
{
	printf("The node has no memory allocated");
	return FALSE;
}
}

/*function to search node from the global linked list and return a pointer to the variable */
sNode *search_snode(sptr startnode , RM_ScanHandle *scanHandle)
{
    sptr prevnode;
    sptr currnode;

    prevnode=NULL;
    currnode=startnode;

    while(currnode!=NULL && currnode->scanHandle!=scanHandle)
    {
        prevnode=currnode;
        currnode=currnode->nextSNode;
    }

    return currnode;
}

/* function to search for a node in the global linked list and delete it from the list */
bool delete_snode(sptr *startnode , RM_ScanHandle *scanHandle)
{
sptr tnode;
sptr prevnode;
sptr currnode;

prevnode=NULL;
currnode=*startnode;

while(currnode!=NULL && currnode->scanHandle!=scanHandle)
{
        prevnode=currnode;
        currnode=currnode->nextSNode;
}

if(currnode!=NULL)
{
	tnode=currnode;
	if(prevnode==NULL)
	{
		*startnode=currnode->nextSNode;
	}
	else
	{
		prevnode->nextSNode=currnode->nextSNode;
	}
	free(tnode);
	return TRUE;
}
else
{
	printf("The Node doesnt exist");
	return FALSE;
}
}


/* this does nothing for record intializer manager */
extern RC initRecordManager (void *mgmtData)
{
return RC_OK;
}

/* this does nothing for record manager shutdown */
extern RC shutdownRecordManager ()
{
return RC_OK;
}

/* Function to create a new table with the given schema and render it as a page*/
extern RC createTable (char *name, Schema *schema)
{
Globalschema=*schema;
char file[100]={'\0'};
int i,pos=0;
strcpy(file,name);
strcat(file,".txt");
createPageFile(file);
BM_PageHandle *pg=MAKE_PAGE_HANDLE();
BM_BufferPool *bm=MAKE_POOL();
initBufferPool(bm,file,1,RS_FIFO,NULL);
pinPage(bm,pg,0);
//have to do some changes here
for(i=0;i < schema->numAttr;i++)
{
	pos+=sprintf(pg->data+pos,"Numattr-%d,DataType[%d]-%d,Typelength[%d]=%d",schema->numAttr,i,schema->dataTypes[i],i,schema->typeLength[i]);
}
markDirty(bm,pg);
unpinPage(bm,pg);
forceFlushPool(bm);
shutdownBufferPool(bm);
return RC_OK;
}

/* Access a existing table from the page and add it to the global linkedlist of open tables */
extern RC openTable (RM_TableData *rel, char *name)
{
char file[100]={'\0'};
Schema *sc=(Schema *)malloc(sizeof(Schema));
*sc=Globalschema;
rel->schema=sc;
strcpy(file,name);
strcat(file,".txt");
BM_BufferPool *bm=MAKE_POOL();
initBufferPool(bm,file,4,RS_FIFO,NULL);
rel->name=name;
rel->mgmtData=bm;
return RC_OK;
}

/* Access a existing table from the page and delete it from the global linkedlist of open tables */
extern RC closeTable (RM_TableData *rel)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
shutdownBufferPool(bm);
free(bm);
freeSchema(rel->schema);
return RC_OK;
}

/* Function to delete the table from the page itself and also destroy the page */
extern RC deleteTable (char *name)
{
char file[100]={'\0'};
strcpy(file,name);
strcat(file,".txt");
destroyPageFile(file);
return RC_OK;
}

/* Function to get the actual no of tuples from the table even the repeated records but omit the deleted records which are blanked out */
extern int getNumTuples (RM_TableData *rel)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
BM_PageHandle *ph=MAKE_PAGE_HANDLE();
SM_FileHandle *fh=(SM_FileHandle *)bm->mgmtData;
int i;
int nooftuples=0;
PageNumber pgno=1;
int pagelength;
    while(pgno < fh->totalNumPages)//Loop through all the page files.
    {
      pinPage(bm,ph,pgno);
      pagelength=strlen(ph->data);
      if(pagelength > 0)
      {
          for(i=0;i < PAGE_SIZE;i++)
          {
		if(ph->data[i]=='|')
		{
               		nooftuples=nooftuples+1;
		}
          }
      }
       unpinPage(bm,ph);
      pgno++;
  }
    free(ph);
    return nooftuples;
}



// handling records in a table
/* insert record given into the table and hence into the pagefile */
extern RC insertRecord (RM_TableData *rel, Record *record)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
BM_PageHandle *ph=MAKE_PAGE_HANDLE();
int slot=0;
PageNumber pgno=1;
int pagelength,totalrecordlength;
SM_FileHandle *fh=(SM_FileHandle *)bm->mgmtData;
totalrecordlength=getRecordSize(rel->schema);
    
    while(pgno < fh->totalNumPages)
    {
      pinPage(bm,ph,pgno);
      pagelength=strlen(ph->data);
      if(PAGE_SIZE-pagelength > totalrecordlength)
      {
          slot=pagelength/totalrecordlength;
          unpinPage(bm,ph);
          break;
      }
       unpinPage(bm,ph);
      pgno++;
  }
  if(slot==0)
  {
      pinPage(bm,ph,pgno + 1);
      unpinPage(bm,ph);
  }

pinPage(bm,ph,pgno);
char *tmp=NULL;
tmp=ph->data;
tmp=tmp+strlen(ph->data);
strcpy(tmp,record->data);
markDirty(bm,ph);
unpinPage(bm,ph);
RID rid;
rid.page=pgno;
rid.slot=slot;
record->id=rid;
free(ph);
return RC_OK;
}

/*Delete a record identified from the Record id and also black out the space occupied by it by # which is for the tombstone concept*/
extern RC deleteRecord (RM_TableData *rel, RID id)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
BM_PageHandle *ph=MAKE_PAGE_HANDLE();
int recsize=getRecordSize(rel->schema);
int frame=id.slot,i;
PageNumber pno=id.page;
char *data=NULL;
char *datatodelete=NULL;
if(pinPage(bm,ph,pno)==RC_OK)
{
	data=ph->data;
	datatodelete= data + recsize*frame;
	for(i=0;i<recsize;i++)
	{
		datatodelete[i]='#';//Tombstone delete
	}
	markDirty(bm,ph);
	unpinPage(bm,ph);
}
else
{
return RC_IM_KEY_NOT_FOUND;
}
free(ph);
return RC_OK;
}

/*Function to get the record from the page file by accessing the start of the record and using the schema to get the bytes for the records and retrieve it and store it in record and return it. */
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
BM_PageHandle *ph=MAKE_PAGE_HANDLE();
int recsize=getRecordSize(rel->schema);
int frame=id.slot;
PageNumber pno=id.page;
char *data=NULL;
char *datatoupdate=NULL;
if(pinPage(bm,ph,pno)==RC_OK)
{
	data=ph->data;
	datatoupdate= data + recsize*frame;
	strncpy(record->data,datatoupdate,recsize);
	record->id=id;
	markDirty(bm,ph);
	unpinPage(bm,ph);
}
else
{
return RC_IM_KEY_NOT_FOUND;
}
free(ph);
return RC_OK;
}

/* Access the page file and the location of the record and replace it with the new record in the same space */
extern RC updateRecord (RM_TableData *rel, Record *record)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
BM_PageHandle *ph=MAKE_PAGE_HANDLE();
int recsize=getRecordSize(rel->schema);
RID id=record->id;
int frame=id.slot;
PageNumber pno=id.page;
char *data=NULL;
char *datatoupdate=NULL;
if(pinPage(bm,ph,pno)==RC_OK)
{
	data=ph->data;
	int point=recsize*frame;
	datatoupdate= data + point;
	strncpy(datatoupdate,record->data,recsize);
	markDirty(bm,ph);
	unpinPage(bm,ph);
}
else
{
return RC_IM_KEY_NOT_FOUND;
}
free(ph);
return RC_OK;
}



// scans
/* open the scan and add it to the list of open scans which are globally present */
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
BM_PageHandle *ph=MAKE_PAGE_HANDLE();
SM_FileHandle *fh=(SM_FileHandle *)bm->mgmtData;
sNode *sn=(sNode *)malloc(sizeof(sNode));
scan->rel=rel;
scan->mgmtData= cond;

sn->page=1;
sn->totalnumPages=fh->totalNumPages;
sn->totalrecordlength=getRecordSize(rel->schema);
sn->totalrecordsinpage=0;
sn->slot=1;
sn->ph=ph;
sn->scanHandle=scan;
insert_snode(&stptr,scan,sn);
return RC_OK;
}

/* scan thought the open scans and get the record's attribute values based on the condition given */
extern RC next (RM_ScanHandle *scan, Record *record)
{
bool marker=FALSE;
    sNode *sn=search_snode(stptr,scan);
    Expr *e=(Expr *)scan->mgmtData;
    RM_TableData *relation=scan->rel;
    int pg_len;
    Value **column=(Value **)malloc(sizeof(Value *));
    RID r_ids;
    *column=NULL;
    if(e==NULL)
    {
        
      if(sn->page < sn->totalnumPages)
      {
          pinPage(relation->mgmtData,sn->ph,sn->page);
          pg_len=strlen(sn->ph->data);
          sn->totalrecordsinpage=pg_len/sn->totalrecordlength;
          if(sn->slot < sn->totalrecordsinpage)
          {
              r_ids.page=sn->page;
              r_ids.slot=sn->slot;
              getRecord(relation,r_ids,record);
              sn->slot++;
          }
          else
          {
            //snode page
             // snode slot
              sn->page+=1;
              sn->slot=1;
          }
          // unpin the page
          unpinPage(relation->mgmtData,sn->ph);
	  //after returning records for no conditions
          free(column[0]);
          free(column);
          return RC_OK; 
      }
      else
      {
          free(column[0]);
          free(column);
	  //There are not more tuples in the table.
          return RC_RM_NO_MORE_TUPLES;
      }
    }
    else
    {
    Expr *secexp,*l,*r;
    Operator *scan_crit;
    Operator *sec_scancrit;
    scan_crit=e->expr.op;
    if(scan_crit->type==OP_COMP_SMALLER)
    {
        r=scan_crit->args[1];
		l=scan_crit->args[0];
        while(sn->page < sn->totalnumPages)
      	{
          pinPage(relation->mgmtData,sn->ph,sn->page);
          pg_len=strlen(sn->ph->data);
          sn->totalrecordsinpage=pg_len/sn->totalrecordlength;
          while(sn->slot < sn->totalrecordsinpage)
          {
              
              r_ids.slot=sn->slot;
			  r_ids.page=sn->page;
			  getRecord(relation,r_ids,record);
              getAttr(record,relation->schema,r->expr.attrRef,column);
              if(relation->schema->dataTypes[r->expr.attrRef]==DT_INT)
	      {
              if(column[0]->v.intV < l->expr.cons->v.intV)
              {
                 sn->slot++;
                 unpinPage(relation->mgmtData,sn->ph);
                 marker=TRUE;
                 break;
              }
              }
	      if(relation->schema->dataTypes[r->expr.attrRef]==DT_FLOAT)
	      {
              if(column[0]->v.floatV < l->expr.cons->v.floatV)
              {
                 sn->slot++;
                 unpinPage(relation->mgmtData,sn->ph);
                 marker=TRUE;
                 break;
              }
              }
              sn->slot++;
          }
          if(marker==TRUE)
            break;
          else
          {
           sn->page++;
           sn->slot=1;
           unpinPage(relation->mgmtData,sn->ph);
          }
      	}
      }
      else if(scan_crit->type==OP_COMP_EQUAL)
      {
        l=scan_crit->args[0];
        r=scan_crit->args[1];
        while(sn->page < sn->totalnumPages)
      	{
          pinPage(relation->mgmtData,sn->ph,sn->page);
          pg_len=strlen(sn->ph->data);
          sn->totalrecordsinpage=pg_len/sn->totalrecordlength;
          while(sn->slot < sn->totalrecordsinpage)
          {
              r_ids.page=sn->page;
              r_ids.slot=sn->slot;
              getRecord(relation,r_ids,record);
              getAttr(record,relation->schema,r->expr.attrRef,column);
	      DataType dt=relation->schema->dataTypes[r->expr.attrRef];
              if(dt==DT_INT){
              if(column[0]->v.intV == l->expr.cons->v.intV)
              {
                 sn->slot++;
                 unpinPage(relation->mgmtData,sn->ph);
                 marker=TRUE;
                 break;
              }
              }
              else if(dt==DT_STRING)
                {
                if(strcmp(column[0]->v.stringV , l->expr.cons->v.stringV)==0)
                {
                 sn->slot++;
                 unpinPage(relation->mgmtData,sn->ph);
                 marker=TRUE;
                 break;
                }
                }
              else if(dt==DT_FLOAT)
                {
                if(column[0]->v.floatV == l->expr.cons->v.floatV)
                {
                 unpinPage(relation->mgmtData,sn->ph);
                 marker=TRUE;
		 sn->slot++;
                 break;
                }
                }

               sn->slot++;
           }
          if(marker==TRUE)
            break;
          else
          {
	   unpinPage(relation->mgmtData,sn->ph);
           sn->slot=1;
	   sn->page++;
          }
      }
     }
	//case for evaluating greater than condition using not and <
      else if(scan_crit->type==OP_BOOL_NOT)      
      {
          secexp=e->expr.op->args[0];
          sec_scancrit=secexp->expr.op;
          r=sec_scancrit->args[0];
	  l=sec_scancrit->args[1];

        if(sec_scancrit->type==OP_COMP_SMALLER)
	{
	      while(sn->page < sn->totalnumPages)
	      {
	          pinPage(relation->mgmtData,sn->ph,sn->page);
	          pg_len=strlen(sn->ph->data);
	          sn->totalrecordsinpage=pg_len/sn->totalrecordlength;
	          while(sn->slot < sn->totalrecordsinpage)
	          {
		      r_ids.slot=sn->slot;
	              r_ids.page=sn->page; 
	              getRecord(relation,r_ids,record);
	              getAttr(record,relation->schema,r->expr.attrRef,column);
		      DataType dt=relation->schema->dataTypes[r->expr.attrRef];
	              if(dt==DT_INT)
		      {
	              	if(column[0]->v.intV > l->expr.cons->v.intV)
	              	{
	              	   sn->slot++;
	              	   unpinPage(relation->mgmtData,sn->ph);
	              	   marker=TRUE;
			   break;
	              	}
        	      }
		      if(dt==DT_FLOAT)
		      {
	              	if(column[0]->v.floatV > l->expr.cons->v.floatV)
	              	{
	              	   sn->slot++;
	              	   unpinPage(relation->mgmtData,sn->ph);
	              	   marker=TRUE;
			   break;
	              	}
        	      }
	              sn->slot++;
            	  }
          	if(marker==TRUE)
     	        { break; }
          	else
          	{
           	sn->page++;
                sn->slot=1;
		unpinPage(relation->mgmtData,sn->ph);
	        }
     	     }
	//return RC_OK;
      	}
	else
	return RC_OK;
	free(*column);
	}
free(column);
if(marker==TRUE)
{
return RC_OK;
//condition match found for a tuple.
}
else
{
    return RC_RM_NO_MORE_TUPLES;
} 
}
}

/* close the scan and delete the scan from the global list of active scans */
extern RC closeScan (RM_ScanHandle *scan)
{
delete_snode(&stptr,scan);
return RC_OK;
}

// dealing with schemas
extern int getRecordSize (Schema *schema)
{
DataType *dts=schema->dataTypes;
int size=0,i;
int num=schema->numAttr;
int *len=schema->typeLength;
for(i=0;i<num;i++)
{
	DataType dt=*(dts + i);
	if(dt==DT_INT)
	{
	size+=sizeof(int);
	}
	else if(dt==DT_FLOAT)
	{
	size+=sizeof(float);
	}
	else if(dt==DT_BOOL)
	{
	size+=sizeof(bool);
	}
	else if(dt==DT_STRING)
	{
	size+=len[i];
	}

}
return size+num;
}

/* create a new schema based on the array of attribute names and types given .*/
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
Schema *sch = (Schema*) malloc(sizeof(Schema));
sch->numAttr = numAttr;
sch->keySize = keySize;
sch->keyAttrs = keys; 
sch->attrNames = attrNames;
sch->typeLength = typeLength;
sch->dataTypes = dataTypes;


return sch;
}

/* delete the space allocated for the schema */
extern RC freeSchema (Schema *schema)
{
free(schema);
return RC_OK;
}

// dealing with records and attribute values
/* create a new record witht eh schema given and initialize the data to empty char */
extern RC createRecord (Record **record, Schema *schema)
{
DataType *dts=schema->dataTypes;
int num=schema->numAttr;
int *len=schema->typeLength;
int i,mem=0;
char *data;
// a little change here to get record size
mem=getRecordSize(schema);
data=(char*)malloc(mem);
for(i=0;i<num;i++)
{
data[i]='\0';
}
*record=(Record *)malloc(sizeof(Record));
record[0]->data=data;
return RC_OK;

}
/* free the space allocated to the record and delete it hence*/
extern RC freeRecord (Record *record)
{
free(record);//free record pointer.
return RC_OK;
}


/* update a record with the attribute value given in value and write to page*/
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
DataType *dts=schema->dataTypes;
int *len=schema->typeLength;
int num=schema->numAttr;
int tmp;
int addr=1;
if(attrNum<num)
{
int i=0,j;
for(i=0;i<attrNum;i++)
{
	if (dts[i] == DT_INT) {
	addr += sizeof(int);
	} else if (dts[i]== DT_FLOAT) {
	addr += sizeof(float);
	} else if (dts[i]== DT_STRING) {
	addr += len[i];
	} else if (dts[i]== DT_BOOL) {
	addr += sizeof(bool);
	} 
}
addr+=attrNum;

char *setaddr;
if(attrNum==0)
{
	setaddr =record->data ;
	setaddr[0]='|';
	setaddr++;
}
else
{
	setaddr=record->data+addr;
	(setaddr-1)[0]=',';
}
if (value->dt == DT_INT) 
{
	sprintf(setaddr,"%d",value->v.intV);
	while(strlen(setaddr)!=sizeof(int))
        {
	        strcat(setaddr,"0");
        }

	for (i=0,j=strlen(setaddr)-1 ; i < j;i++,j--)
	{
		tmp=setaddr[i];
        	setaddr[i]=setaddr[j];
        	setaddr[j]=tmp;
    	}

} 
else if (value->dt == DT_FLOAT) 
{
	sprintf(setaddr,"%f",value->v.floatV);
	while(strlen(setaddr)!=sizeof(float))
        {
	        strcat(setaddr,"0");
        }

	for (i=0,j=strlen(setaddr)-1 ; i < j;i++,j--)
	{
        	tmp=setaddr[i];
        	setaddr[i]=setaddr[j];
        	setaddr[j]=tmp;
	}
} 
else if (value->dt == DT_BOOL) 
{
	sprintf(setaddr,"%i",value->v.boolV);
} 
else if (value->dt == DT_STRING) 
{ 
	sprintf(setaddr,"%s",value->v.stringV);
}
return RC_OK;
}
else
{
RC_message = "attrNum is greater than the available number of Attributes";
return RC_RM_NO_MORE_TUPLES;
}
}


/*get the attribute value of the specific record based on the schema and return it using the value*/
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) 
{
DataType *dts = schema->dataTypes;
int *len = schema->typeLength;
int num = schema->numAttr;
int addr = 1,i;
char *data = record->data;
Value *val = (Value*) malloc(sizeof(Value));
//tmp1[100]='\0';
char *tmp=NULL;
if (attrNum < num) 
{
	
	for (i = 0; i < attrNum; i++) 
	{
	if (dts[i] == DT_INT) 
	{
		addr += sizeof(int);
	} else if (dts[i]== DT_FLOAT) {
		addr += sizeof(float);
	} else if (dts[i]== DT_STRING) { 
		addr += len[i];
	} else if (dts[i]== DT_BOOL) {
		addr += sizeof(bool);
	} 
	}
addr+=attrNum;
int attrsize = 0;
int dt = *(dts + i);	
if (dt == DT_INT) 
{
	val->dt = DT_INT;
	attrsize += sizeof(int);
	tmp=malloc(attrsize+1);
} 
else if (dt == DT_FLOAT) 
{
	val->dt = DT_FLOAT;
	attrsize += sizeof(float);
	tmp=malloc(attrsize+1);
} 
else if (dt == DT_BOOL) 
{
	val->dt = DT_BOOL;
	attrsize += sizeof(bool);
	tmp=malloc(attrsize+1);
} 
else if (dt == DT_STRING) 
{ 
	val->dt = DT_STRING;
	attrsize += *(len + i);
	tmp=malloc(attrsize+1);
	for(i=0;i <= attrsize ; i++)
        {
        	tmp[i]='\0';
        }
}

strncpy(tmp, data + addr, attrsize); 
tmp[attrsize]='\0';

if (val->dt == DT_INT) {
	val->v.intV = atoi(tmp);
} 
else if (val->dt == DT_FLOAT) 
{
	val->v.floatV = (float) *tmp;
} 
else if (val->dt == DT_BOOL) 
{
	val->v.boolV = (bool) *tmp;
} 
else if (val->dt == DT_STRING) 
{
	val->v.stringV=malloc(attrsize*sizeof(char));
	strcpy(val->v.stringV,tmp);
}
value[0]=val;
free(tmp);
return RC_OK;
}
RC_message = "attrNum is greater than the available number of attributes";
return RC_RM_NO_MORE_TUPLES;

}





