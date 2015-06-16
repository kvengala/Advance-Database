CC=gcc

FLAG= -c 

all:record_mgr1_case1 record_mgr1_case2 clean 
	

record_mgr1_case1: test_assign3_1.o record_mgr.o expr.o rm_serializer.o buffer_mgr.o storage_mgr.o buffer_mgr_stat.o dberror.o
	$(CC) test_assign3_1.o  record_mgr.o expr.o rm_serializer.o buffer_mgr.o storage_mgr.o buffer_mgr_stat.o dberror.o -o testcase1 


record_mgr1_case2: test_expr.o 
	$(CC) test_expr.o  record_mgr.o expr.o rm_serializer.o buffer_mgr.o storage_mgr.o buffer_mgr_stat.o dberror.o -o testcase2


test_assign3_1.o: test_assign3_1.c
	$(CC) $(FLAG) test_assign3_1.c  
	echo "Compiling"
	touch test_assign3_1.o

test_expr.o: test_expr.c
	$(CC) $(FLAG) test_expr.c  
	echo "Compiling"
	touch test_expr.o



record_mgr.o: record_mgr.c
	$(CC) $(FLAG) record_mgr.c
	echo "compiling"
	touch record_mgr.o

expr.o: expr.c
	$(CC) $(FLAG) expr.c
	echo "compiling"
	touch expr.o


rm_serializer.o: rm_serializer.c
	$(CC) $(FLAG) rm_serializer.c
	echo "compiling"
	touch rm_serializer.o

buffer_mgr.o: buffer_mgr.c
	$(CC) $(FLAG) buffer_mgr.c 
	echo "Compiling"
	touch buffer_mgr.o
storage_mgr.o: storage_mgr.c
	$(CC) $(FLAG) storage_mgr.c -lm
	echo "compiling"
	touch storage_mgr.o


buffer_mgr_stat.o: buffer_mgr_stat.c
	$(CC) $(FLAG) buffer_mgr_stat.c 
	echo "compiling"      
	touch buffer_mgr_stat.o

dberror.o: dberror.c 
	$(CC) $(FLAG) dberror.c 
	echo "Compiling"
	touch dberror.o

clean:
	rm -r *.o 
