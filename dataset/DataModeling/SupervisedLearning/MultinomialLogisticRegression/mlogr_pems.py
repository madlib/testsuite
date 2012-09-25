#!/usr/bin/env python
import sys, os, zipfile, shutil
import time

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
    parser.add_option("-s", "--sourfile", action="store", dest="sourfile", type="string")
    parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
    parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
    (options, args) = parser.parse_args()
    
    
    #variable def
    sour_file = options.sourfile          #the source file        
    dest_file = options.destfile          #where target sql file stores
    tb_name = options.tablename           #get table name
        
    tmp_dir = "tmp." + time.strftime("%Y%m%d%H%M%S", time.localtime())
        
    #check tmp dir
    if os.path.isdir(tmp_dir): shutil.rmtree(tmp_dir)


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
    if zipfile.is_zipfile(sour_file):
        z = zipfile.ZipFile(sour_file, 'r')
        for f in z.namelist():
            z.extract(f, path = tmp_dir)
            
        
    print("Processing file PEMS_train and PEMS_trainlabels ...")
    procRawfile(output, os.path.join(tmp_dir,"PEMS_train"), os.path.join(tmp_dir,"PEMS_trainlabels"))
                
    print("Processing file PEMS_test and PEMS_testlabels ...")
    procRawfile(output, os.path.join(tmp_dir,"PEMS_test"), os.path.join(tmp_dir,"PEMS_testlabels"))
    

    output.write("\.\n")
    output.flush()
    output.close()
        
    #delete tmp file
#shutil.rmtree(tmp_dir)


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, attr_file, label_file):
    global g_count, Cache_L_id, Cache_L_x, Cache_L_y, Cache_size
    
    line_id = 0
    
    L_label = open(label_file,"r").read().strip()[1:-1].split(" ")
    
    
    input = open(attr_file,"r")
    while True:
        line = input.readline()
        if not line: break
                
        """
        if len(L) != 137710:
            print(L)
            line_id += 1
            continue
        """
            
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
        
            #attributes
            L = line.strip()[1:-1].split(";")
            
            tmp_L = []
	    tmp_L.append(1)
            for i in range(len(L)):
                inner_L = L[i].strip().split(" ")
                for j in range(len(inner_L)):
                    tmp_L.append(float(inner_L[j]))
                    j += 1
                i += 1
                        
            Cache_L_x.append(tmp_L)
                
            #proc label
            Cache_L_y.append(L_label[line_id])
            line_id += 1

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
