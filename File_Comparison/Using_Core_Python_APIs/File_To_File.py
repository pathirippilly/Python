import os
import sys
import time

# functions

def fileCount(file_list=[]):
    row_cnt=len(file_list)
    col_cnt=len(file_list[0].split(","))
    yield row_cnt,col_cnt

def fileDetailing(file_path="",file_list=[]):
    print "FILE DETAILING"
    print "**************"
    print ""

    print "FILE_NAME:{}".format(file_path)
    print"SIZE: {} bytes".format(os.path.getsize(file_path))
    cnt = (x for x in next(fileCount(file_list)))
    print "RECORD_COUNT: {}".format(next(cnt))
    print "COLUMN_COUNT: {}".format(next(cnt))
    print ""
def fullValidation(output_path="",source=[],target=[]):
    compared = map(lambda x: (x[0].strip(), x[1].strip()), filter(lambda data: data[0] != data[1], zip(source, target)))
    if len(compared) > 0:
        error_report = "{}\\file_diff.{}".format(output_path, int(time.time()))
        print error_report
        print "FULL DATA VALIDATION FAILED!!"
        print""
        print "Please find the conflict records of source and target in {}".format(error_report)
        with open(error_report, 'w+') as f:
            f.write("SOURCE|TARGET\n")
            for line in compared:
                f.write("{}|{}\n".format(line[0], line[1]))

    else:
        print "FULL DATA VALIDATION SUCCESSFULLY COMPLETED"

# Variable initialisation

  flag=1
output_path = sys.argv[3]

# File Existence Check
print sys.argv
print len(sys.argv)
if len(sys.argv)==4:
    for fil in sys.argv[:3]:
        if not os.path.isfile(fil):
            print "{} is not a file or file does not exist.Please enter a valid file".format(fil)
            flag=0
            break
    if not os.path.exists(output_path):
        flag=0
        print "{} does not exist".format(output_path)
else:
    flag=0
    print "invalid Number of Arguments Provided"

# Main Validation
if flag==1:
    # Opening files and adding the contents in to a list
    with open(sys.argv[1]) as src,open(sys.argv[2]) as tgt:
        src = list(src)
        tgt = list(tgt)

    # file detailing
    print "SOURCE"
    print ""
    fileDetailing(sys.argv[1],src)
    print "Target"
    print ""
    fileDetailing(sys.argv[2], tgt)

    # file Validation
    print "SOURCE vs TARGET : COUNT VALIDATION"
    print ""
    cnt_src = (x for x in next(fileCount(src)))
    cnt_tgt= (x for x in next(fileCount(tgt)))
    if next(cnt_src) == next(cnt_tgt):
        print "RECORD COUNT TEST SUCCEEDED"
        if next(cnt_src) == next(cnt_tgt):
            print "COLUMN COUNT CHECK SUCCEEDED"
            print""
            print"SOURCE vs TARGET:FULL DATA VALIDATION"
            print""
            fullValidation(output_path,src,tgt)