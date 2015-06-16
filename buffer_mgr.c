#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr_stat.h"
#include "pthread.h"



/***
Struct BF_plnd 
This contains the linked list of all the existing buffer pools
this will get intialized in the first init buffer manager call and a then it holds the various buffer pools in the as nodes in the list ***/

typedef struct BF_plnd
{
    void *bf_plptr;
    int IOread;
	void *bf_pgdtl;
    int IOwrite;
    struct BF_plnd *nxtnode;
} BF_plnd,*ndptr;

static ndptr start_ptr=NULL;

/***
Struct Buff_pgdtl
This one contains all the additional values and details needed to maintain the buffer pool and also maintain the replacement strategy ***/

typedef struct Buff_pgdtl
{
    char *pageframes;
    PageNumber pagenums;
    bool dirty;
    int fixcounts;
    int replacementWeight;
    int clockcount;
    long double time;
} Buff_pgdtl;


Buff_pgdtl *init_bffdtls(const int numPages)
{
    int i,j;
    Buff_pgdtl *temporary_pagedtl=NULL;
    temporary_pagedtl=(Buff_pgdtl *)calloc(numPages,sizeof(Buff_pgdtl));
    if(temporary_pagedtl!=NULL)
    {
        for(i=0;i < numPages;i++)
        {
            	char *data;
    		data=(char *)malloc(PAGE_SIZE);
   		if(data!=NULL)
    		{
        		for(j=0;j < PAGE_SIZE ; j++)
            		data[j]='\0';
    		}
	    (temporary_pagedtl+i)->pageframes=data;
            temporary_pagedtl[i].fixcounts=0;
            temporary_pagedtl[i].dirty=FALSE;
            temporary_pagedtl[i].pagenums=NO_PAGE;
	    temporary_pagedtl[i].replacementWeight=0;
    	    temporary_pagedtl[i].clockcount=0;
    	    temporary_pagedtl[i].time=0;	
        }
    }
    return temporary_pagedtl;
}
/***
insert_buffpool 
This does the job of adding a new buffer pool to the existing linkeded list which is maintained by BF_plnd ***/



static long double utime=-1000;



bool insert_buffpool(ndptr *stnode ,  void *bf_plptr , void *buffer_page_handle)
{
    bool return_value=FALSE;
    ndptr new_ptr;
    ndptr prev_ptr;
    ndptr curr_ptr;
    static pthread_mutex_t insertnode=PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&insertnode);
    new_ptr =(ndptr)malloc(sizeof(BF_plnd));
    if(new_ptr!=NULL)
    {
        new_ptr->bf_plptr=bf_plptr;
        new_ptr->bf_pgdtl=buffer_page_handle;
        new_ptr->IOread=0;
        new_ptr->IOwrite=0;
        new_ptr->nxtnode=NULL;

        prev_ptr=NULL;
        curr_ptr =*stnode;

        while (curr_ptr!=NULL )
        {
            prev_ptr=curr_ptr;
            curr_ptr=curr_ptr->nxtnode;
        }
        if(prev_ptr==NULL)
        {
            *stnode=new_ptr;
        }
        else
        {
            prev_ptr->nxtnode=new_ptr;
        }
        return_value=TRUE;
    }
   else
    {
        printf("Memory not available");
    }
    pthread_mutex_unlock(&insertnode);
    return return_value;
}

/***
Print_buffpool 
finds the pool in the linked list and prints it to be displayed to the user ***/

void print_buffpool(ndptr startptr)
{
    ndptr prev_ptr;
    ndptr curr_ptr;
    BM_BufferPool *bf_node;
    prev_ptr=NULL;
    curr_ptr=startptr;
    if(curr_ptr==NULL)
    {
        printf("List is empty");
    }
    else
    {
        while(curr_ptr!=NULL)
        {
            prev_ptr=curr_ptr;
            curr_ptr=curr_ptr->nxtnode;
            bf_node=(BM_BufferPool *)prev_ptr->bf_plptr;

            printf("%s ->  ",bf_node->pageFile);
        }
    }
}

/***
Delete_buffpool 
This also located the required pool in the linkedlist and deletes the pool space from the list hence freeing the memory ***/

bool delete_buffnode(ndptr *nodeptr ,  void *bf_plptr )
{
    ndptr tem_ptr;
    ndptr prev_ptr;
    ndptr curr_ptr;
    static pthread_mutex_t deletenode=PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&deletenode);
    prev_ptr=NULL;
    curr_ptr=*nodeptr;


    while(curr_ptr!=NULL && curr_ptr->bf_plptr!=bf_plptr)
    {
        prev_ptr=curr_ptr;
        curr_ptr=curr_ptr->nxtnode;
    }

    if(curr_ptr!=NULL)
    {
        tem_ptr=curr_ptr;

        if(prev_ptr== NULL)
        {
            *nodeptr=curr_ptr->nxtnode;
        }
        else
        {
            prev_ptr->nxtnode=curr_ptr->nxtnode;
        }
        free(tem_ptr);
    }
    else
    {
        printf("Item not in the list");
    }
    pthread_mutex_unlock(&deletenode);
    return TRUE;

}

/***
search_buffpool 
Based on the Buffer pool details given it searches the list for the pool and returns a pointer to the required pool ***/

BF_plnd *search_buffpool(ndptr nodeptr,  void *bf_plptr )
{
    ndptr prev_ptr=NULL;
    ndptr curr_ptr=nodeptr;
    static pthread_mutex_t searchnode=PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&searchnode);
    while(curr_ptr!=NULL)
    {
        if(curr_ptr->bf_plptr==bf_plptr)
        {
            break;
        }
        prev_ptr=curr_ptr;
        curr_ptr=curr_ptr->nxtnode;
    }
    if(curr_ptr==NULL)
    {
        printf("Item not available");
    }
    pthread_mutex_unlock(&searchnode);
    return curr_ptr;
}

/***
Chkisactive 
returns the buffer pool with the filename given so that we will use the exiting pool rather than create a new one ***/

BF_plnd *chkisactive(ndptr stnode,char *filename)
{
    ndptr prev_ptr=NULL;
    ndptr curr_ptr=stnode;
    BM_BufferPool *bufferpool;
    while(curr_ptr!=NULL)
    {
        bufferpool=(BM_BufferPool *)curr_ptr->bf_plptr;
        if(bufferpool->pageFile==filename)
        {
           break;
        }
        prev_ptr=curr_ptr;
        curr_ptr=curr_ptr->nxtnode;
    }
    return curr_ptr;
}

/***
fileusd
Makes sure that only one file with filename is handled by only one buffer pool and 
hence two pools wont be for the same file ***/

int fileusd(ndptr stnode,char *filename)
{
    ndptr prev_ptr=NULL;
    ndptr curr_ptr=stnode;
    BM_BufferPool *bufferpool;
    int count=0;
    while(curr_ptr!=NULL)
    {
        bufferpool=(BM_BufferPool *)curr_ptr->bf_plptr;
        if(bufferpool->pageFile==filename)
        {
         count++;
        }
        prev_ptr=curr_ptr;
        curr_ptr=curr_ptr->nxtnode;
    }
    return count;
}

/******  Buffer Pool Functions *******/

/***
initBufferPool 
creates a new buffer pool with numPages page frames using the page replacement strategy strategy. The pool is used to cache pages from the page file with name pageFileName. Initially, all page frames should be empty. The page file should already exist, i.e., this method should not generate a new page file. stratData can be used to pass parameters for the pagefilehandle. This utilizes the checkisactive and insertpool functions ***/


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
    static pthread_mutex_t init_mutex=PTHREAD_MUTEX_INITIALIZER;
    RC return_value=RC_OK;
    Buff_pgdtl *buffpg;
    BF_plnd *buffer_node;
    BM_BufferPool *temp_pool;
    SM_FileHandle *sh;
    pthread_mutex_lock(&init_mutex); 
    buffer_node=chkisactive(start_ptr,pageFileName);
    if(buffer_node==NULL)
    {
	printf("entering null part\n");
        sh=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));
        if(sh==NULL)
        {
            return RC_FILE_HANDLE_NOT_INIT;
        }
        openPageFile(pageFileName,sh);	
        bm->mgmtData=sh;	
	bm->numPages=numPages;
        bm->strategy=strategy;
        bm->pageFile=pageFileName;
        
	buffpg=init_bffdtls(numPages);
	if(buffpg!=NULL)
	{ return_value=RC_OK; }
	else
	{ return_value=RC_FILE_HANDLE_NOT_INIT; }        
        
	if(insert_buffpool(&start_ptr,bm,buffpg)==FALSE)
        return_value=RC_FILE_HANDLE_NOT_INIT;
    }
    else
       {
            temp_pool=buffer_node->bf_plptr;
	    bm->numPages=numPages;
            bm->pageFile=pageFileName;	
            sh=temp_pool->mgmtData;
     	    bm->mgmtData=sh;
	    bm->strategy=strategy;
            buffpg=buffer_node->bf_pgdtl;
            if(insert_buffpool(&start_ptr,bm,buffpg)==FALSE)
            return_value=RC_FILE_HANDLE_NOT_INIT;
       }
      pthread_mutex_unlock(&init_mutex);
return return_value;
}

/***
shutdownBufferPool 
destroys a buffer pool. This method frees up all resources associated with buffer pool by calling delete function. But it first checks if the buffer pool contains any dirty pages, then these pages should be written back to disk before destroying the pool. It also checks whether the pool contains any frames which are currently being used by any one using the fixcount. If found pinned it thows error or else it deletes the pool. ***/

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    static pthread_mutex_t shutdown_mutex=PTHREAD_MUTEX_INITIALIZER;
    RC return_value=RC_WRITE_FAILED;
    int i ,*page_numbers, *fixcounts;
    bool pinned=FALSE;
    Buff_pgdtl *page_details;
    BF_plnd *buffer_node;
    char *frame;
    int curr_access_file;
    pthread_mutex_lock(&shutdown_mutex);
    fixcounts=getFixCounts(bm);
    page_numbers=getFrameContents(bm);
    for(i=0 ; i < bm->numPages ; i++)
    {
        if(fixcounts[i] > 0)
        {
            pinned=TRUE;
        }
    }
    free(fixcounts);
    if(pinned==FALSE)
    {
        return_value=RC_OK;
        buffer_node=search_buffpool(start_ptr,bm);
        curr_access_file=fileusd(start_ptr,bm->pageFile);
        if(buffer_node!=NULL)
        {
            page_details=buffer_node->bf_pgdtl;
            if(page_details!=NULL)
            for(i=0;i<bm->numPages;i++)
            {
                frame=page_details[i].pageframes;
                if(page_details[i].dirty==TRUE)
                {
			if(writeBlock(page_details[i].pagenums,bm->mgmtData,frame)==RC_OK)
			{ return_value=RC_OK; }
			else
			{ return_value=RC_WRITE_FAILED;
			}
                }
                if(curr_access_file == 1) 
			free(frame);
            }
            if(curr_access_file == 1) 
		free(page_details);
            page_details=NULL;
            delete_buffnode(&start_ptr,bm);
        }
    if(curr_access_file == 1)
	closePageFile(bm->mgmtData);
	free(bm->mgmtData);
  }
  pthread_mutex_unlock(&shutdown_mutex);

    return return_value;
}
/***
forceFlushPool 
causes all dirty pages (with fix count 0) from the buffer pool to be written to disk. ***/

RC forceFlushPool(BM_BufferPool *const bm)
{
    static pthread_mutex_t force_flushmutex=PTHREAD_MUTEX_INITIALIZER;
    RC return_value=RC_OK;
    int i;
    Buff_pgdtl *page_details;
    BF_plnd *buffer_node;
    char *frame;
    pthread_mutex_lock(&force_flushmutex);
    buffer_node=search_buffpool(start_ptr,bm);
    if(buffer_node!=NULL)
        {
            page_details=buffer_node->bf_pgdtl;
            for(i=0;i<bm->numPages;i++)
            {
                frame=page_details[i].pageframes;
                if(page_details[i].dirty==TRUE && page_details[i].fixcounts==0)
                {
                   return_value=writeBlock(page_details[i].pagenums,bm->mgmtData,frame);
                   page_details[i].dirty=FALSE;
                   buffer_node->IOwrite++;
                }
            }
        }
    pthread_mutex_unlock(&force_flushmutex);
    return return_value;
}

/********* Statistics Functions *********/

/***
The getFrameContents 
function returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame. An empty page frame is represented using the constant NO_PAGE. ***/

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    PageNumber i,*pgnum=NULL;
    Buff_pgdtl *buffpg;
    ndptr buf_node=search_buffpool(start_ptr,bm);
    if(buf_node!=NULL)
    {
        pgnum=(PageNumber *)calloc(bm->numPages,sizeof(PageNumber));
        buffpg=buf_node->bf_pgdtl;
        if(buffpg!=NULL){
        for(i=0;i < bm->numPages;i++)
        {
            pgnum[i]=buffpg[i].pagenums;
        }
        }else
          {
              free(buffpg);
              buffpg=NULL;
          }

    }
    return pgnum;
}

/***
The getDirtyFlags 
function returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty. Empty page frames are considered as clean. ***/

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    int i;
    bool  *dir_flags;
    Buff_pgdtl *buffpg;
    ndptr buf_node=search_buffpool(start_ptr,bm);
    if(buf_node!=NULL)
    {
        dir_flags=(bool *)calloc(bm->numPages,sizeof(bool));
        buffpg=buf_node->bf_pgdtl;
        if(buffpg!=NULL){
        for(i=0;i < bm->numPages;i++)
        {
            dir_flags[i]=buffpg[i].dirty;
        }
        }else
          {
              free(buffpg);
              buffpg=NULL;
          }
    }
return dir_flags;
}

/***
The getNumReadIO 
function returns the number of pages that have been read from disk since a buffer pool has been initialized. You code is responsible to initializing this statistic at pool creating time and update whenever a page is read from the page file into a page frame. ***/

int getNumReadIO (BM_BufferPool *const bm)
{
    int  Ioread=0;
    ndptr buf_node=search_buffpool(start_ptr,bm);
    if(buf_node!=NULL)
    {
        Ioread=buf_node->IOread;
    }

    return Ioread;

}

/***
getNumWriteIO 
returns the number of pages written to the page file since the buffer pool has been initialized. ***/

int getNumWriteIO (BM_BufferPool *const bm){
    int  IOwrite=0;
    ndptr buffer_node=search_buffpool(start_ptr,bm);
    if(buffer_node!=NULL)
    {
        IOwrite=buffer_node->IOwrite;
    }
    return IOwrite;
}

/***
The getFixCounts 
function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. Return 0 for empty page frames. ***/

int *getFixCounts (BM_BufferPool *const bm)
{
int i,*fixcounts;
Buff_pgdtl *buffpg;
ndptr buffer_node=search_buffpool(start_ptr,bm);
    if(buffer_node!=NULL)
      {
          fixcounts=(int *)calloc(bm->numPages,sizeof(int));
          buffpg=buffer_node->bf_pgdtl;
          if(buffpg!=NULL){
          for(i=0 ; i < bm->numPages ; i++)
          {
              fixcounts[i]=buffpg[i].fixcounts;
          }
          }
          else
          {
              free(buffpg);
              buffpg=NULL;
          }

      }
return fixcounts;
}


/********      Page Management Functions    *******/
/***
markDirty marks a page as dirty alone by assessing the pagefile and marking it dirty.***/

RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {
	int i;
	BF_plnd *pg_node;
	Buff_pgdtl *buffpg;
	pg_node = search_buffpool(start_ptr, bm);
	buffpg = pg_node->bf_pgdtl;
	for (i = 0; i < bm->numPages; i++) {
		if (((buffpg + i)->pagenums) == page->pageNum) {
			(buffpg + i)->dirty = TRUE;
			return RC_OK;
		}
	}
return RC_WRITE_FAILED; 
}

/***
unpinPage unpins the page. The pageNum field of page is used to figure out which page to unpin and we just reduce the fixcount value for the pagefile by one. ***/


RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {

	int tot_pages = bm->numPages, i, k;
	BF_plnd *bf_node;
	bf_node = search_buffpool(start_ptr, bm);
	Buff_pgdtl *page_details;
	page_details = bf_node->bf_pgdtl;
	for (i = 0; i < tot_pages; i++) {
		if ((page_details + i)->pagenums == page->pageNum) 
		{
			
			(page_details + i)->fixcounts -= 1;
			return RC_OK;
		}
	}

	return RC_WRITE_FAILED; 
}

/***
forcePage writes the current content of the page back to the page file on disk
if it is not pinned.***/

RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	BF_plnd *bf_node;
	bf_node = search_buffpool(start_ptr, bm);
	if(bf_node!=NULL)
    {

	if (bm->mgmtData!= NULL) {
		writeBlock(page->pageNum, bm->mgmtData, page->data);
		bf_node->IOwrite++;
	} else {
		return RC_FILE_NOT_FOUND;
	}
    }
	return RC_OK;
}



RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) 
{
	static pthread_mutex_t pin_mutex=PTHREAD_MUTEX_INITIALIZER;
	int r_framenum, tot_pages, i;
	int stat_flag;
	ReplacementStrategy strategy = bm->strategy;
	Buff_pgdtl *lowest_possPage;
	tot_pages = bm->numPages;
	BF_plnd *bf_node;
	pthread_mutex_lock(&pin_mutex);
	bf_node = search_buffpool(start_ptr, bm);
	Buff_pgdtl *page_details;
	page_details = bf_node->bf_pgdtl;
	RC write_value=1, read_value=1;
	int emptyFlag = 1;
	if (page_details != NULL) {

		
		for (i = 0; i < tot_pages; i++) {
		
			if ((page_details + i)->pagenums > -1) { 
				emptyFlag = 0;
	
				if ((page_details + i)->pagenums == pageNum) {
                    (page_details + i)->time=utime++;
                    page->pageNum = pageNum;
					page->data = (page_details + i)->pageframes;
					(page_details + i)->fixcounts+=1;
					if(strategy== RS_LFU)
					page_details->replacementWeight+=1;
					pthread_mutex_unlock(&pin_mutex);
					return RC_OK; 
				}

			}
		} 

		
		for (i = 0; i < tot_pages; i++) {
			if ((page_details + i)->pagenums == -1)
			{
				lowest_possPage = ((page_details + i)); 
				emptyFlag = 1;
				break;
			}
		}

	} else { 
		lowest_possPage = (page_details + 0); 
		lowest_possPage->replacementWeight =
				lowest_possPage->replacementWeight + 1;
		

		(page_details + i)->time=utime++;
		emptyFlag = 1;
	}

	
	if (emptyFlag == 1) {
		page->pageNum = pageNum;
		page->data = lowest_possPage->pageframes;
		lowest_possPage->clockcount=1;
		write_value = RC_OK;
	
		if (lowest_possPage->dirty == TRUE) {
			write_value = writeBlock(pageNum, bm->mgmtData,
					lowest_possPage->pageframes);
					bf_node->IOwrite++;
		}
		if(readBlock(pageNum, bm->mgmtData,lowest_possPage->pageframes)==RC_READ_NON_EXISTING_PAGE)
				{
					read_value=appendEmptyBlock(bm->mgmtData);
					
				}
				else
                    read_value=RC_OK;
        	bf_node->IOread++;
		lowest_possPage->fixcounts += 1;
		lowest_possPage->pagenums = pageNum;
	

	} else { 
		if (strategy == RS_FIFO){
			return FIFO(bm, page, pageNum);
		}else if (strategy == RS_LRU){
			return LRU(bm, page, pageNum);
                }else if(strategy==RS_LFU){
			return LFU(bm,page,pageNum);
		}else if(strategy==RS_CLOCK){
			return CLOCK(bm,page,pageNum);
		}else {
			return RC_WRITE_FAILED; 
		}
	} 

	 pthread_mutex_unlock(&pin_mutex);
	if (write_value == RC_OK && read_value == RC_OK) {
		return RC_OK;
	} else {
		return RC_WRITE_FAILED;
	}
}


Buff_pgdtl *sortWeights(BM_BufferPool * const bm, BF_plnd *bf_node);

/***
FIFO
This does the calculation needed to find the First in page to be replaced and check whether it is pinned. If pinned it finds another page. Then it checks whether dirty and write or deleted based on that. ***/

RC FIFO(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	BF_plnd *bf_node;
	bf_node = search_buffpool(start_ptr, bm);
	Buff_pgdtl *lowest_possPage=NULL;
	lowest_possPage = sortWeights(bm, bf_node);

	if(lowest_possPage==NULL)
    {
        return RC_WRITE_FAILED;
    }
	char *rep_addr;
	rep_addr = lowest_possPage->pageframes;

	RC write_value = RC_OK;
	RC read_value = RC_OK;
	if (lowest_possPage->dirty == TRUE) {
		write_value = writeBlock(lowest_possPage->pagenums, bm->mgmtData, rep_addr);
		lowest_possPage->dirty=FALSE;
		bf_node->IOwrite++;
	}
	read_value = readBlock(pageNum, bm->mgmtData, rep_addr);
	if(read_value==RC_READ_NON_EXISTING_PAGE)
    {
        read_value =appendEmptyBlock(bm->mgmtData);
    }
    bf_node->IOread++;
	page->pageNum  = pageNum;
	page->data = lowest_possPage->pageframes;
    lowest_possPage->pagenums = pageNum;
    lowest_possPage->fixcounts+=1;
	lowest_possPage->replacementWeight = lowest_possPage->replacementWeight + 1;
	if (read_value == RC_OK && write_value == RC_OK)
		return RC_OK; 
	else
		return RC_WRITE_FAILED;
}

/***LRU
This does the calculation needed to find the Least recently used page to be replaced and check whether it is pinned. If pinned it finds another page. Then it checks whether dirty and write or deleted based on that. ***/

RC LRU(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	BF_plnd *bf_node;
	bf_node = search_buffpool(start_ptr, bm);
	Buff_pgdtl *lowest_possPage;
	lowest_possPage = sortWeights(bm, bf_node);
	char *rep_addr;
	rep_addr = lowest_possPage->pageframes;

	RC write_value = RC_OK;
	RC read_value = RC_OK;
	if (lowest_possPage->dirty == TRUE) {
		write_value = writeBlock(page->pageNum, bm->mgmtData, rep_addr);
		lowest_possPage->dirty=FALSE;
		bf_node->IOwrite++;
	}
	read_value = readBlock(pageNum, bm->mgmtData, rep_addr);
	page->pageNum  = pageNum;
	page->data = lowest_possPage->pageframes;
    lowest_possPage->pagenums = pageNum;
    lowest_possPage->fixcounts+=1;
	lowest_possPage->replacementWeight =
    lowest_possPage->replacementWeight + 1;
	lowest_possPage->time =(long double)utime++;
    bf_node->IOread++;
	if (read_value == RC_OK && write_value == RC_OK)
		return RC_OK; 
	else
		return RC_WRITE_FAILED;
}

/***
LFU
This does the calculation needed to find the Least recently used page to be replaced and check whether it is pinned. If pinned it finds another page. Then it checks whether dirty and write or deleted based on that. ***/

RC LFU(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {

	BF_plnd *bf_node;
	bf_node = search_buffpool(start_ptr, bm);
	Buff_pgdtl *lowest_possPage=NULL;
	lowest_possPage = sortWeights(bm, bf_node);
	if(lowest_possPage==NULL)
    {
        return RC_WRITE_FAILED;
    }
	char *rep_addr;
	rep_addr = lowest_possPage->pageframes;


	RC write_value = RC_OK;
	RC read_value = RC_OK;
	if (lowest_possPage->dirty == TRUE) {
		write_value = writeBlock(lowest_possPage->pagenums, bm->mgmtData, rep_addr);
		lowest_possPage->dirty=FALSE;
		bf_node->IOwrite++;
	}
	read_value = readBlock(pageNum, bm->mgmtData, rep_addr);
	if(read_value==RC_READ_NON_EXISTING_PAGE)
    {
        read_value =appendEmptyBlock(bm->mgmtData);
    }
    bf_node->IOread++;
	page->pageNum  = pageNum;
	page->data = lowest_possPage->pageframes;
    lowest_possPage->pagenums = pageNum;
    lowest_possPage->fixcounts+=1;
	lowest_possPage->replacementWeight = 1;
	if (read_value == RC_OK && write_value == RC_OK)
		return RC_OK; 
	else
		return RC_WRITE_FAILED;
}

RC CLOCK(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	BF_plnd *bf_node;
	bf_node = search_buffpool(start_ptr, bm);
	Buff_pgdtl *lowest_possPage=NULL;
	lowest_possPage = sortWeights(bm, bf_node);

	if(lowest_possPage==NULL)
    {
        return RC_WRITE_FAILED;
    }
	char *rep_addr;
	rep_addr = lowest_possPage->pageframes;

	RC write_value = RC_OK;
	RC read_value = RC_OK;

	if (lowest_possPage->dirty == TRUE) {
		write_value = writeBlock(lowest_possPage->pagenums, bm->mgmtData, rep_addr);
		lowest_possPage->dirty=FALSE;
		bf_node->IOwrite++;
	}
	read_value = readBlock(pageNum, bm->mgmtData, rep_addr);
	if(read_value==RC_READ_NON_EXISTING_PAGE)
    {
        read_value =appendEmptyBlock(bm->mgmtData);
    }
    bf_node->IOread++;
	page->pageNum  = pageNum;
	page->data = lowest_possPage->pageframes;
    lowest_possPage->pagenums = pageNum;
    lowest_possPage->fixcounts+=1;
    lowest_possPage->clockcount=1;

	lowest_possPage->replacementWeight = lowest_possPage->replacementWeight + 1;
	if (read_value == RC_OK && write_value == RC_OK)
		return RC_OK; 
	else
		return RC_WRITE_FAILED;


}



/***
SortWeight
This function is used by the FIFO and LFU to sort the Pages based on the value for the replacement strategy. ***/

Buff_pgdtl *sortWeights(BM_BufferPool * const bm, BF_plnd *bf_node) {
	int i;
	int totalPagesInBlock = bm->numPages;
	Buff_pgdtl *bf_page_dtl = bf_node->bf_pgdtl;
	Buff_pgdtl *bf_page_dtl_with_zero_fixcount[totalPagesInBlock];
	int count = 0;


	for (i = 0; i < totalPagesInBlock; i++) {
		bf_page_dtl_with_zero_fixcount[i] = NULL;
		}

	for (i = 0; i < totalPagesInBlock; i++) {
		if ((bf_page_dtl[i].fixcounts) == 0) {
			bf_page_dtl_with_zero_fixcount[count] = (bf_page_dtl+i);
			count++;
		}
	}


    #define sizeofa(array) sizeof array / sizeof array[ 0 ]
    int sizeOfPagesWithFixcountZero = sizeofa(bf_page_dtl_with_zero_fixcount);


	Buff_pgdtl *next_bf_page_dtl;
	Buff_pgdtl *replac_buffpgdtl ;
	replac_buffpgdtl = bf_page_dtl_with_zero_fixcount[0];
	for (i = 0; i < sizeOfPagesWithFixcountZero; i++) {
		next_bf_page_dtl = bf_page_dtl_with_zero_fixcount[i];
		if(next_bf_page_dtl!=NULL){
			if(bm->strategy == RS_FIFO){
				if ((replac_buffpgdtl->replacementWeight) > (next_bf_page_dtl->replacementWeight))
				replac_buffpgdtl = next_bf_page_dtl;
			}
			else if(bm->strategy==RS_LFU){
				if ((replac_buffpgdtl->replacementWeight) > (next_bf_page_dtl->replacementWeight)) 
				{
			replac_buffpgdtl = next_bf_page_dtl;                }
				else if ((replac_buffpgdtl->replacementWeight)==(next_bf_page_dtl->replacementWeight))
				{
					if((replac_buffpgdtl->time < next_bf_page_dtl->time))		
			replac_buffpgdtl = next_bf_page_dtl;			
				}		
			}	
			else if(bm->strategy==RS_LRU){

	            if(replac_buffpgdtl->time > next_bf_page_dtl->time)
				{
					replac_buffpgdtl = next_bf_page_dtl;
				}
			}
			else if(bm->strategy==RS_CLOCK){
			if(replac_buffpgdtl->clockcount==1)
			{  replac_buffpgdtl->clockcount+=1;	}
			else if ((replac_buffpgdtl->replacementWeight) > (next_bf_page_dtl->replacementWeight)) 
			{				
				replac_buffpgdtl = next_bf_page_dtl;

			}			
			}
		}
	}

return replac_buffpgdtl;
}





