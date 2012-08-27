#!/usr/bin/env python
import sys, os, zipfile, shutil

g_count = 0      #global count to generate id
line_limit = 100 #line num to write out

id_L=[]
class_L=[]
attr_L = []


# =======================
# === Function : main ===
# =======================
def main():
    from optparse import OptionParser
    
    #optparser
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--sourfile", action="store", dest="sourfile", type="string",help="input the file name")
    parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
    parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
    (options, args) = parser.parse_args()
    
    #variable def
    sour_file = options.sourfile            #the source file
    dest_file = options.destfile            #where target sql file stores
    tb_name = options.tablename             #get table name
    
    
    #create dest_data file
    if os.path.isfile(dest_file):os.remove(dest_file)
    output = open(dest_file,"w")
    
    output.write("--check table\n");
    output.write("SET client_min_messages TO WARNING;DROP TABLE IF EXISTS "+ tb_name +" CASCADE;\n");
    output.write("\n");
    output.write("--create table\n");
    output.write("create table "+ tb_name +"(id int, attributes int[], class int);\n");
    output.write("\n");
    output.write("COPY "+ tb_name +" from stdin delimiter '#';\n");
    
    
    #parse source data
    procRawfile(output, sour_file)
    
    output.write("\.\n")
    output.write("alter table "+tb_name+" owner to madlibtester;\n")
    output.flush()
    output.close()



# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
    global g_count,line_limit,id_L,class_L,attr_L
    
    input = open(rawfile,"r")
    while True:
        line = input.readline()
        if not line: break                  #when end, then break
        L = line.strip().split("\t")
        
        if 3 != len(L):  continue            #jump bad data

        if len(attr_L) < line_limit :
            #continue processing

            #proc id
            id_L.append(g_count)
            g_count += 1
        
            #proc attr
            attr_L.append(L[2])
        
            #proc class
            class_L.append(L[1])
        
        else :
		    #write out data
            i = 0
            while i < len(attr_L):
				#print("Debug: write out" + str(id_L[i])+ "$" + attr_L[i][:5] +"$"+ class_L[i])
                output.write("%s#{%s}#%s\n" % (id_L[i], attr_L[i][1:-1], class_L[i]))
                i += 1
            
            id_L = []
            attr_L = []
            class_L = []

    if 0 != len(attr_L):
        i = 0
        while i < len(attr_L):
            output.write("%s#{%s}#%s\n" % (id_L[i], attr_L[i][1:-1], class_L[i]))
            i += 1
        
        id_L = []
        attr_L = []
        class_L = []
    
    input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
    main()
