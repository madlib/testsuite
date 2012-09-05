
#!/usr/bin/env python
import sys, os, time, zipfile, shutil

line_limit = 1000   #line limit to write data out (used as a cache)

L_id = []           #cache for trans_id
L_prod = []         #cache for product


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
    parser.add_option("-D", "--dataset", action="store", dest="dataset", type="string", help="-D webview or -D pos")
    parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
    (options, args) = parser.parse_args()
    
    
    #variable def
    sour_data = options.sourdata            #the source file
    dest_data = options.destdata            #where target sql file stores
    data_set = options.dataset.strip()      #get dataset name: BMS-WebView-2.dat.gz / BMS-POS.dat.gz
    tb_name = options.tablename             #get table name
    
    dataset_name = ""
    if "webview" == data_set : dataset_name = "BMS-WebView-2.dat.gz"
    elif "pos" == data_set : dataset_name = "BMS-POS.dat.gz"
    else :
        print("Input dataset name error!")
        return
    
    tmp_dir = "tmp_" + time.strftime("%Y%m%d%H%M%S",time.localtime(time.time()))
    
    #check tmp dir
    if os.path.isdir(tmp_dir): shutil.rmtree(tmp_dir)
    
    
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
    
    
    #parse source data
    if zipfile.is_zipfile(sour_data):
        z = zipfile.ZipFile(sour_data,"r")
        for f in z.namelist():
            nL = os.path.split(f)
            if dataset_name == nL[len(nL) - 1].strip():
                zfile = z.extract(f, path=tmp_dir)              #extract target .gz file
                tar_file = zfile[:zfile.rindex(".gz")]          #get target file name
                os.system("gzip -d "+zfile)                     #uncompress
                print("Processing file " + tar_file + " ...")
                procRawfile(output, tar_file)                      #processing
    else :
        print("Input file type error!")
        return

    
    output.write("\.\n")
    output.write("alter table "+tb_name+" owner to madlibtester;\n")
    output.flush()
    output.close()
    
    #delete tmp file
    shutil.rmtree(tmp_dir)


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
    global line_limit, L_id, L_prod
    
    input = open(rawfile)
    while True:
        line = input.readline()
        if not line: break
        L = line.strip().split("\t")
        
        if len(L_id) >= line_limit:
            #flush cache
            i = 0
            while i < line_limit:
                output.write("%s#%s\\n\n" % (L_id[i], L_prod[i]))
                i += 1
                    
            L_id = []
            L_prod = []
    
            L_id.append(L[0])
            L_prod.append(L[1])
        else:
            #add data to cache
            L_id.append(L[0])
            L_prod.append(L[1])
    
    if 0 != len(L_id):
        i = 0
        j = len(L_id)
        while i < j:
            output.write("%s#%s\\n\n" % (L_id[i], L_prod[i]))
            i += 1

        L_id = []
        L_prod = []
    
    input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
    main()
