About Script
************
Version: 2.0
Interpretor supported : Python 2.7.x
Maximum average execution time for data up to 40 Million: <50 seconds
Supported OS : Windows


Whats there in script:
**********************

This is V2.0 version of script:
This will compare two user input files (Source and Target files)and perform:

1.Column Count Validation
2.Row Count Validation or Record Count validation
3.Duplicate Check 
4.Line by Line Record validation (Even if duplicates are there in Source and Target Files, it ignores the duplicate and equate line by line)
5.Conflict records of step 4 (if any) and Duplicate records of step 3 (if any) are logged in to files and will be saved under a output directory given by user

duplicate output file will be named as : dup.<epochtime>
File containing duplicated records will be named as : file_diff.<epochtime>

6.As of now it supported delimiter is 'comma' (But will include more delimitters in near future)

Usage (CLI execution):

python File_To_File.py <full qualified path of source file> <full qualified path of target file> <output directory to which output files should be saved>

example:

python File_To_File.py A:\source.txt A:\tgt.txt A:\output

Note:
1. Here I am running from where .py file is present.
2. A:\source.txt is the source and A:\tgt.txt is the target
3. A:\output is the output directory to which output duplicate file and ouput conflict file will be saved


What going to Come:
*******************
1.Support More Delimiters 
2.Changes for adapting it to run on Linux/Unix OS as well
3.User input for columns to be sorted if any 
4.Null Check , Columns need to be provided by the user
