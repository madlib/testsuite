#!/usr/bin/env python
import sys, os, shutil
import zipfile
import time

g_count = 0      #global count to generate id

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
  c = 0
  file = ""
  if zipfile.is_zipfile(sour_file):
    z = zipfile.ZipFile(sour_file,"r")
    for f in z.namelist():
      if (f.find('.') >= 0) and (f[f.rindex('.'):len(f)] == '.data'):  #only process target ".data" data
        base_f = os.path.basename(f)
        if int(base_f[1:base_f.rindex(".")]) > c:
          c = int(base_f[1:base_f.rindex(".")])
          file = f
          
    t_file = z.extract(file, path= tmp_dir)
    print("Processing file "+ file)
    procRawfile(output, t_file)

  output.write("\.")
  output.flush()
  output.close()

  #delete tmp file
  shutil.rmtree(tmp_dir)
  
  print("Finished!")


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
  global g_count, Cache_L_id, Cache_L_x, Cache_L_y, Cache_size

  #compute sum
  input = open(rawfile,"r")
  while True:
    line = input.readline()
    if not line: break
    if line.find("?") >=0 : continue
    
    L = line.strip().split(",")
    if 5410 != len(L): continue
      
    #Check Cache Size
    if len(Cache_L_id) >= Cache_size:
        for i in range(Cache_size):
            output.write("%s#{%s}#%s\\n\n" % (Cache_L_id[i], str(Cache_L_x[i])[1:-1], Cache_L_y[i]))
            i += 1
        
        Cache_L_id = []
        Cache_L_x  = []
        Cache_L_y  = []
    else:
        #id
        Cache_L_id.append(g_count)
        g_count += 1
      
        #attributes
        tmp_L = []
	tmp_L.append(1)
        for i in range(5408):
            tmp_L.append(float(L[i]))
            i += 1
        Cache_L_x.append(tmp_L)

        #class
        if("active" == L[5408].strip()): Cache_L_y.append("1")
        elif("inactive" == L[5408].strip()): Cache_L_y.append("0")
        else : Cache_L_y.append("0")

  #Check Cache Size
  if 0 != len(Cache_L_id):
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
