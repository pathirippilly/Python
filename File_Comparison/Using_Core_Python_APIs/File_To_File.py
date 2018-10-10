import os
import sys
import time
from functools import wraps

# functions

def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print ("Total time running %s: %s seconds" %
               (function.func_name, str(t1-t0))
               )
        return result
    return function_timer

def fileCount(file_list=[]):
    row_cnt=len(file_list)
    col_cnt=len(file_list[0].split(","))
    yield row_cnt,col_cnt
def fileSize(file_path):
    sz=os.path.getsize(file_path)
    sz_str="{} bytes".format(sz)
    if sz>1000000000:
        sz = round(sz/1000000000.0,2)
        sz_str = "{} GB".format(sz)
    elif sz>1000000:
        sz = round(sz/1000000.0,2)
        sz_str = "{} MB".format(sz)
    elif sz>1000:
        sz = round(sz/1000.0,2)
        sz_str = "{} KB".format(sz)
    yield sz_str


def fileDetailing(file_path="",file_list=[]):
    print "FILE DETAILING"
    print "**************"
    print ""
    print "FILE_NAME:{}".format(file_path)
    print"SIZE: {}".format(next(fileSize(file_path)))
    cnt = (x for x in next(fileCount(file_list)))
    print "RECORD_COUNT: {}".format(next(cnt))
    print "COLUMN_COUNT: {}".format(next(cnt))
    print ""
def fullValidation(output_path="",source=[],target=[]):
    source=sorted(source,key=lambda x : x.split(",")[0])
    target=sorted(target,key=lambda x : x.split(",")[0])

    compared = map(lambda x: (x[0].strip(), x[1].strip()), filter(lambda data: data[0] != data[1], zip(source, target)))
    if len(compared) > 0:
        error_report = "{}\\file_diff.{}".format(output_path, int(time.time()))
        print error_report
        print "FULL DATA VALIDATION FAILED!!"
        print""
        print "Please find the conflict records of source and target in {}".format(error_report)
        print ""
        with open(error_report, 'w+') as f:
            f.write("SOURCE|TARGET\n")
            for line in compared:
                f.write("{}|{}\n".format(line[0], line[1]))

    else:
        print "FULL DATA VALIDATION SUCCESSFULLY COMPLETED"
@fn_timer

def dupFind(dup_list=[],output_path=""):
    dup_list=sorted(dup_list,key=lambda x : x.split(",")[0])
    t0 = time.time()
    duplicates=set((x,dup_list.count(x)) for x in filter(lambda rec : dup_list.count(rec)>1,dup_list))
    t1 = time.time()
    print "time taken for preparing duplicate list is {}".format(str(t1-t0))

    dup_report="{}\dup.{}".format(output_path, int(time.time()))

    print "Please find the duplicate records  in {}".format(dup_report)
    print ""
    with open(dup_report, 'w+') as f:
        f.write("RECORD|DUPLICATE_COUNT\n")
        for line in duplicates:
            f.write("{}|{}\n".format(line[0], line[1]))

def completeTest(source=[],target=[]):
    if len(set(target)) < len(target) and len(set(source)) < len(source):
        print "Both Source: {} and Target: {} is having duplicates".format(sys.argv[1], sys.argv[2])
        print ""
        print "Handling Duplication of Source:"
        print ""
        dupFind(source, output_path)
        print "Handling Duplication of Target:"
        print ""
        dupFind(target, output_path)
        print"SOURCE vs TARGET:FULL DATA VALIDATION IGNORING THE DUPLICATES OF BOTH SOURCE AND TARGET"
        print""
        fullValidation(output_path, list(set(source)), list(set(target)))
    elif len(set(target)) < len(target):
        print "Target : {} is having duplicate records".format(sys.argv[2])
        dupFind(target, output_path)
        print"SOURCE vs TARGET:FULL DATA VALIDATION IGNORING THE DUPLICATES OF TARGET"
        print""
        fullValidation(output_path, source, list(set(target)))
    elif len(set(source)) < len(source):
        print "Source  : {} is having duplicate records".format(sys.argv[1])
        dupFind(source, output_path)
        print"SOURCE vs TARGET:FULL DATA VALIDATION IGNORING THE DUPLICATES OF SOURCE"
        print""
        fullValidation(output_path, list(set(source)), target)
    else :
        print "NO DUPLICATES ARE FOUND IN SOURCE AND TARGET"
        print ""
        fullValidation(output_path,source,target)


# Variable initialisation

flag=1
output_path = sys.argv[3]

# File Existence Check
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
        src = map(lambda x : x.strip(),list(src))
        tgt = map(lambda x : x.strip(),list(tgt))



    # file detailing
    print "SOURCE"
    print ""
    fileDetailing(sys.argv[1],src)
    print "Target"
    print ""
    fileDetailing(sys.argv[2], tgt)

    # file Validation
    print "SOURCE vs TARGET : COLUMN COUNT VALIDATION"
    print ""
    if len(src[0].split(",")) == len(tgt[0].split(",")):
        print "COLUMN COUNT CHECK SUCCEEDED"
        print""
        print "SOURCE vs TARGET : RECORD COUNT VALIDATION"
        print ""
        if len(set(src)) == len(set(tgt)):

            print "RECORD COUNT TEST SUCCEEDED IGNORING THE DUPLICATES IF ANY"
            print""

            print"SOURCE vs TARGET:FULL DATA VALIDATION"
            print""
            completeTest(src,tgt)

        else:
            print "RECORD COUNT TEST FAILED"
            print ""

            print"SOURCE vs TARGET:FULL DATA VALIDATION"
            print""

            completeTest(src, tgt)
    else:
        print "COLUMN COUNT CHECK FAILED!!"
        print""
        if len(src[0].split(",")) > len(tgt[0].split(",")):
            print "SOURCE IS HAVING MORE NUMBER OF COLUMNS THAN TARGET!!"
            print""
        if len(src[0].split(",")) < len(tgt[0].split(",")):
            print "TARGET IS HAVING MORE NUMBER OF COLUMNS THAN SOURCE!!"
            print""