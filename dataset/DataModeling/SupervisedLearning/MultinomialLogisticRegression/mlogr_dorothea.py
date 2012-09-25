#!/usr/bin/env python
import sys, os, shutil

g_count = 0                 #global count to generate id/Users/bjcoe/Documents/emc/dataload/sourcedata/regression/logisticr/data/recordlink/mlogr_recordlink.py

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
    parser.add_option("-s", "--sourfile", action="store", dest="sourfile", type="string", help="path/sourcefile")
    parser.add_option("-l", "--labelfile", action="store", dest="labelfile", type="string", help="path/sourcefile")
    parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string", help="path/destfile.sql")
    parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string", help="madlibtestdata.tablename")
    parser.add_option("-D", "--dimension", action="store", dest="dimension", type="int")
    (options, args) = parser.parse_args()
    
    
    #variable def
    sour_file = options.sourfile
    label_file= options.labelfile
    dest_file = options.destfile
    tb_name = options.tablename
    dimension = options.dimension
    
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
    
    print("Processing file: "+ sour_file +" and "+ label_file)
    procRawfile(output, sour_file, label_file, dimension)
    
    
    output.write("\.")
    output.flush()
    output.close()
    
    print("Finished!")


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, sour_file, label_file, dimension):
    global g_count, Cache_L_id, Cache_L_x, Cache_L_y, Cache_size
    
    input_sour = open(sour_file, "r")
    input_label = open(label_file, "r")
    while True:
        line_sour = input_sour.readline().strip()
        line_label = input_label.readline().strip()
        if ((not line_sour) and (not line_label)): break
        L = line_sour.split(" ")
        
        
        #chech cache size
        if len(Cache_L_id) >= Cache_size:
            #write out
            for i in range(Cache_size):
                output.write("%s#{%s}#%s\\n\n" % (Cache_L_id[i], str(Cache_L_x[i])[1:-1], Cache_L_y[i]))
                i += 1
            
            Cache_L_id = []
            Cache_L_x  = []
            Cache_L_y  = []
        else:
            #Continue process
            
            #proc id
            Cache_L_id.append(g_count)
            g_count = g_count + 1
        
            #proc L_sour as x
            length = len(L)
            i = 0
            d = 0
            tmp_L = []
	    tmp_L.append(1)
            while d < dimension:
                if (i < length) and (int(L[i].strip()) == (d + 1)):
                    tmp_L.append(1)
                    i = i + 1
                else:
                    tmp_L.append(0)
                d = d + 1
    
            Cache_L_x.append(tmp_L)
                
            #proc line_label as y
            if line_label.strip() == "1": Cache_L_y.append("1")
            else: Cache_L_y.append("0")
    
    #check if there still exists values
    if len(Cache_L_id) != 0:
        for i in range(len(Cache_L_id)):
            output.write("%s#{%s}#%s\\n\n" % (Cache_L_id[i], str(Cache_L_x[i])[1:-1], Cache_L_y[i]))
            i += 1
        Cache_L_id = []
        Cache_L_x = []
        Cache_L_y = []

    input_sour.close()
    input_label.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
    main()
