
#include<stdio.h>
#include<stdlib.h>
#include "dberror.h"
#include "dberror.h"
#include <sys/stat.h>

char *pageFile;
/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void)
{
}

int fsize(FILE *fp){
    int prev=ftell(fp);
    fseek(fp, 0L, SEEK_END);
    int sz=ftell(fp);
sz=sz-prev;
    fseek(fp,prev,SEEK_SET); //go back to where we were
    return sz;
}
extern RC createPageFile (char *fileName)
{
FILE *fp=NULL;
fp=fopen(fileName,"ab+");
if(fp==NULL)
{
return RC_FILE_NOT_FOUND;
}
else
{
int i=0;
for(i=0;i<4096;i++)
{
fwrite("\0",1,1,fp);
}
fclose(fp);
return RC_OK;
}
}
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
FILE *fp=NULL;
size_t size;
if((fp=fopen(fileName,"r+"))==NULL)
{
printf("file not found 1 \n");
return RC_FILE_NOT_FOUND;
}
else
{
size = fsize(fp);
printf("%zd file size\n",size);

int Totnum=size/PAGE_SIZE;
fHandle->totalNumPages=Totnum;
fHandle->curPagePos=0;
fHandle->mgmtInfo=fp;
fHandle->fileName=fileName;
return RC_OK;
}
}
extern RC closePageFile (SM_FileHandle *fHandle)
{
fclose(fHandle->mgmtInfo);
}
extern RC destroyPageFile (char *fileName)
{
int status;
status=remove(fileName);
if(status==0)
{
printf("File deleted\n");
return RC_OK;
}
else
{
printf("file not deleted and not found \n");
return RC_FILE_NOT_FOUND;
}
}

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,pageNum*4096,SEEK_SET);
size_t t;
t=fread(memPage,1,4096,fp);
if(t==4096)
{return RC_OK;
}
else
{return RC_READ_NON_EXISTING_PAGE;
}
}
extern int getBlockPos (SM_FileHandle *fHandle)
{
int t=fHandle->curPagePos;
return t;
}
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,0,SEEK_SET);
size_t t;
t=fread(memPage,1,4096,fp);
fHandle->curPagePos=1;
if(t==4096)
{return RC_OK;
}
else
{return RC_READ_NON_EXISTING_PAGE;
}

}
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,(fHandle->curPagePos-1)*4096,SEEK_SET);
size_t t;
t=fread(memPage,1,4096,fp);
if(t==4096)
{return RC_OK;
}
else
{return RC_READ_NON_EXISTING_PAGE;
}

}
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,(fHandle->curPagePos)*4096,SEEK_SET);
size_t t;
fHandle->curPagePos=fHandle->curPagePos+1;
t=fread(memPage,1,4096,fp);
if(t==4096)
{return RC_OK;
}
else
{return RC_READ_NON_EXISTING_PAGE;
}

}
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,(fHandle->curPagePos+1)*4096,SEEK_SET);
size_t t;
fHandle->curPagePos=fHandle->curPagePos+2;
t=fread(memPage,1,4096,fp);
if(t==4096)
{return RC_OK;
}
else
{return RC_READ_NON_EXISTING_PAGE;
}

}
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,(fHandle->totalNumPages-1)*4096,SEEK_SET);
size_t t;
t=fread(memPage,1,4096,fp);
fHandle->curPagePos=fHandle->totalNumPages;
if(t==4096)
{return RC_OK;
}
else
{return RC_READ_NON_EXISTING_PAGE;
}

}
/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fp,pageNum*4096,SEEK_SET);
size_t t;
t=fwrite(memPage,1,4096,fp); 
fHandle->curPagePos=pageNum+1;
if(t==4096)
{
return RC_OK;
}
else
{
return RC_WRITE_FAILED;
}

}
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
FILE *fp=fHandle->mgmtInfo;
size_t t;
fseek(fp,(fHandle->curPagePos)*4096,SEEK_SET);
t=fwrite(memPage,1,sizeof(memPage),fp); 
fHandle->totalNumPages=fHandle->totalNumPages+1;
fHandle->curPagePos=fHandle->curPagePos+1;
if(t==4096)
{
return RC_OK;
}
else
{
return RC_WRITE_FAILED;
}
}
extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{
FILE *fp=fHandle->mgmtInfo;
fseek(fHandle->mgmtInfo,(fHandle->totalNumPages)*4096,SEEK_SET);
int i=0;
char a[4096];
for(i=0;i<4096;i++)
{
a[i]='\0';
}
size_t t=fwrite(a,1,4096,fp);
fHandle->totalNumPages=fHandle->totalNumPages+1;
fHandle->curPagePos=fHandle->totalNumPages;
if(t==4096)
{return RC_OK;}
else
{return RC_WRITE_FAILED;
}
}
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
FILE *fp=fHandle->mgmtInfo;
while(fHandle->totalNumPages<=numberOfPages)
{
if(fHandle->totalNumPages==numberOfPages)
return RC_OK;
else
{
fseek(fHandle->mgmtInfo,(fHandle->totalNumPages)*4096,SEEK_SET);int i=0;
char a[4096];
for(i=0;i<4096;i++)
{
a[i]='\0';
}
size_t t=fwrite(a,1,4096,fp);
fHandle->totalNumPages=fHandle->totalNumPages+1;
fHandle->curPagePos=fHandle->totalNumPages;
if(t!=4096)
{return RC_WRITE_FAILED;}
}
}
}

