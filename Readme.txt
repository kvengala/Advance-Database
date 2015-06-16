**************************Readme**********************************
* Project name		 :Index Manager                              *
*                                                                    *
* Project members	 :Reeve Choksi - A20320185 (Team Leader)     *
*                	  Shivakumar Vinayagam - A20341139           *
*                	  Vengalathur Srikanth Kashyap - A20325618   *
*                         Rejul James - A20345321		     *
******************************************************************
Project Description : 

We have created an index manager. The index manager is used for creating index for the records. A B+ tree index is implemented. 
The B+tree index is backed up by a page file and the pages of the index are accessed by the buffer manager. The index should be small and should support  trees with smaller fan-outs. Pointers to the intermediate node is represented by the page number of the page where it is 
stored in. 
The leaf has the following property 
Leaf split: In case the leaf node needs to split and n is even, the left node gets the extra key.
Non Leaf split: If the non leaf node has to be split and the value of n is odd the node is not split evenly. Here the middle value that has to be inserted is taken from the right node.
Leaf underflow: In case o a leaf underflow, the B tree will redistribute the values to its siblings. If this fails, then the node is merged with one of the siblings.


The program was tested with valgrind and memory leaks was low and all the
pointers are handled properly.

Test Case:
=========
Our make file will create one testcases: testcase1 which is for
the test case test_assign3_1.c.Testcase1 displays the test file made by you 

MAKEFILE DESCRIPTION
====================
The make file will compile the file in the directory and then create a executable file testcase. 
Execution of the testcase 
Execution Steps: 
===============

        Step 1: Move to directory were code and make file is kept
                Command: cd ~/assignment3/
        Step 2: Execute the make file  
		Command: make
	Step 3: Run testcase1 file to check main functionalities
     		Command: ./testcase1


Functional implementations:
===========================

Structures created:

struct tree_list
This datastructure is used to create a list of b+tree indexes. and when the close operation is done, the tree is written to the disk.

struct record
This is the datastructure to hold the record for each index. In this case the record is the Rid for each tuple that has been created 
using the record manager.

struct node
This datastructure is used to create the B+tree index. This datastructure has the parent node and the leaf nodes.

Functions implemented:

node * insert

This function is used to create the index and the record in the B+tree. The leaf nodes are also inserted using this function.

node * insert_into_node

This function is invoked inside the insert function. This is used to find which node is free and then to insert the index and the record.

node * insert_into_node_after_splitting

The size of the leaf nodes are at 2 hence if the size of the node increases by the value then the nodes gets split and then the records are inserted.

node * delete_entry

This routine is used to delete an entry from the tree. the node as well as the record associated with the index 

int cut

This subroutine is used to split the tree when it the number of values in a node increases by 2.

int get_left_index

This routine is used to get the number of left index in the b tree and this is used to search for a specific index in the b+tree.

node * remove_entry_from_node

This routine is used to remove a value from the node. generally in delete functions

node * adjust_root

If the incoming index value is higher than the root node then the root node is adjusteed so as to accomodate the tree and also to maintain the height of the tree.

node * coalesce_nodes

This routine is used to join the nodes in case multiple nodes occupy the same node.

node * redistribute_nodes

This routine is used to redistribute the nodes in case of leaf underflow.

node * make_node

Routine to create a node for the b+tree index.

node * make_leaf

Routine that is used to create leaf nodes for the b+tree index.

node * find_leaf

The routine to search for keys in the leaf nodes.

record * find

This function is used forsearch operation in the index.

record * make_record

This function is used to place a record for a node in the index.

node * insert_into_leaf

This This routine is used to insert record in leaf node.

node * start_new_tree

The start new tree routine creates a new routine.

int get_neighbor_index

This function is used to get the value of neighbour's index.



extern RC initIndexManager 

This function  initiates an index manager. Creates a file from the buffer manager for the index.

extern RC shutdownIndexManager

This function is used to close the index manager. This also writes the index into the file.

 
extern RC openTreeScan

This routine is used to open an existing tree.

extern RC nextEntry

This routine invokes the Btreehandle to load the key and data into the tree.

extern RC closeTreeScan

This routine will close an open tree.

extern RC getNumNodes

This gives the number of nodes present in the tree.

extern RC getNumEntries

This routine is used to get the number of entries in the index.

extern RC getKeyType

This routine is used to obtain the datatype of the key used in the B+tree.

BTreeHandle *search

This routine searches for a particular key in the B+tree index.

extern RC deleteBtree 

This routine is used to delete the B+treee index.



  

