#!/usr/bin/env python
import sys, os, shutil

g_count = 0                 #global count to generate id

Cache_L_id = []             #cache for id
Cache_L_x = []              #cache for indenpendent value x
Cache_L_y = []              #cache for dependent value y

Cache_size = 1000           #cache size


# =======================
# === Function : main ===
# =======================
def main():
    from optparse import OptionParser
    
    #optparser
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--sourfile1", action="store", dest="sourfile1", type="string")
    parser.add_option("-S", "--sourfile2", action="store", dest="sourfile2", type="string")
    parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
    parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
    (options, args) = parser.parse_args()
    
    
    #variable def
    sour_file1 = options.sourfile1         #the source file
    sour_file2 = options.sourfile2         #the source file
    dest_file = options.destfile           #where target sql file stores
    tb_name = options.tablename            #get table name


    #create dest_data file
    if os.path.isfile(dest_file):os.remove(dest_file)
    output = open(dest_file,"w")

        
    output.write("--check table\n")
    output.write("DROP TABLE IF EXISTS "+ tb_name +" CASCADE;\n")
    output.write("\n")
    output.write("--create table\n")
    output.write("create table "+ tb_name +"(id int, x float8[], y int);\n")
    output.write("\n")
    output.write("alter table "+tb_name+" owner to madlibtester;\n")
    output.write("copy "+ tb_name +" from stdin delimiter '#';\n")

        
    #parse source data
    print("Processing file "+ sour_file1)
    procRawfile(output, sour_file1)
    
    print("Processing file " + sour_file2)
    procRawfile(output, sour_file2)
    
    output.write("\.\n")
    output.flush()
    output.close()
    

    print("Finished!")


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
    global g_count
    global Cache_L_id, Cache_L_x, Cache_L_y, Cache_size
    
    input = open(rawfile)
    while True:
        line = input.readline()
        if not line: break
        L = line.strip().split(",")
        
        #check cache size
        if len(Cache_L_id) >= Cache_size:   #write result
            for i in range(Cache_size):
                output.write("%s#{%s}#%s\\n\n" % (Cache_L_id[i], str(Cache_L_x[i])[1:-1], Cache_L_y[i]))
                i += 1
            Cache_L_id = []
            Cache_L_x  = []
            Cache_L_y  = []
        
        else :                              #process data
            #id
            Cache_L_id.append(g_count)
            g_count += 1
        
            #attributes L[0~616]
            tmp_L = []
	    tmp_L.append(1)
            for i in range (len(L)-1):
                tmp_L.append(float(L[i]))
                i += 1
            Cache_L_x.append(tmp_L)
                
            #proc L[617]
            Cache_L_y.append(int(float(L[-1])))

    #check whether there still exist values
    if len(Cache_L_id) != 0 :
        for i in range(len(Cache_L_id)):
            output.write("%s#{%s}#%s\\n\n" % (Cache_L_id[i], str(Cache_L_x[i])[1:-1], Cache_L_y[i]))
            i += 1
        
        Cache_L_id = []
        Cache_L_x  = []
        Cache_L_y  = []

        
    input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
    main()
