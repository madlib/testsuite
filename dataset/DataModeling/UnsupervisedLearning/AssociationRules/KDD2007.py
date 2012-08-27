
#!/usr/bin/env python
import sys, os, time, zipfile, shutil


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
    output.write("COPY "+ tb_name +" from stdin delimiter ',';\n");
    
    
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
    
    input = open(rawfile)
    while True:
        line = input.readline()
        if not line: break
        output.write(line.strip() + "\\n\n")
    
    input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
    main()
