
#!/usr/bin/env python
import sys, os, time, zipfile, shutil

g_count = 0      #global count to generate id

line_limit = 1000   #cache line

L_id = []           #cache for id
L_prod = []         #cache for production


# =======================
# === Function : main ===
# =======================
def main():
    from optparse import OptionParser
    
    #optparser
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-s", "--sourdata", action="store", dest="sourdata", type="string")
    parser.add_option("-d", "--destdata", action="store", dest="destdata", type="string")
    parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
    (options, args) = parser.parse_args()
    
    
    #variable def
    sour_data = options.sourdata            #the source file
    dest_data = options.destdata            #where target sql file stores
    tb_name = options.tablename             #get table name
    
    
    #create dest_data file
    if os.path.isfile(dest_data):os.remove(dest_data)
    output = open(dest_data,"w")
    
    output.write("--check table\n");
    output.write("DROP TABLE IF EXISTS "+ tb_name +" CASCADE;\n");
    output.write("\n");
    output.write("--create table\n");
    output.write("create table "+ tb_name +"(trans_id int, product text);\n");
    output.write("\n");
    output.write("COPY "+ tb_name +" from stdin delimiter '#';\n");
    
    
    #processing file
    print("Processing file " + os.path.split(sour_data)[1] +" ...")
    procRawfile(output, sour_data)
    
    output.write("\.\n")
    output.write("alter table "+tb_name+" owner to madlibtester;\n")
    output.flush()
    output.close()
    print("Finished!")
    

# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
    global g_count, line_limit, L_id, L_prod
    
    input = open(rawfile)
    while True:
        line = input.readline()
        if not line: break
        
        L = line.strip().split(",")
        
        if 0 == len(L) : 
	    print("Not Legitimate: "+line)
	    continue
        
        if len(L_id) >= line_limit :
            #flush data
            
            i = 0
            while i < line_limit:
                j = len(L_prod[i])
                k = 0
                while k < j:
                    if "?" != L_prod[i][k] : output.write("%s#%s\\n\n" % (L_id[i], L_prod[i][k]))
                    k += 1
                i += 1
            
            L_id = []
            L_prod = []
                
            #continue processing data
            #proc products
            tmp_L = []
            i = 0
            while i < len(L):
                tmp_L.append(L[i])
                i += 1
            L_prod.append(tmp_L)
            
            #proc id
            L_id.append(g_count)
            g_count += 1
        
                
        else :
            #processing data
            #proc products
            tmp_L = []
            i = 0
            while i < len(L):
                tmp_L.append(L[i])
                i += 1
            L_prod.append(tmp_L)
            
            #proc id
            L_id.append(g_count)
            g_count += 1
            
                
    if 0 != len(L_id) :
        i = 0
        m = len(L_id)
        while i < m :
            j = len(L_prod[i])
            k = 0
            while k < j :
                if "?" != L_prod[i][k] : output.write("%s#%s\\n\n" % (L_id[i], L_prod[i][k]))
                k += 1
            
            i += 1
                
        L_id = []
        L_prod = []

    input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
    main()
