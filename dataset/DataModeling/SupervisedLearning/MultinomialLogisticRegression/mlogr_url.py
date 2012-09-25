#!/usr/bin/env python
import sys, os, shutil
import tarfile
import time

g_count = 0      #global count to generate id

attr_L = []      #global attr cache
id_L = []        #global id cache
label_L = []     #global label cache

line_limit = 1000  #line limit to write cache out




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
  parser.add_option("-D", "--dimension", action="store", dest="dimension", type="int")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  parser.add_option("-w", "--daylist", action="store", dest="daylist", type="string")
  (options, args) = parser.parse_args()


  #variable def
  sour_file = options.sourfile          #the source file
  dest_file = options.destfile          #where target sql file stores
  dimension = options.dimension         #get dimension for attributes
  tb_name = options.tablename           #get table name
  day_list = options.daylist.strip().split(",")
  
    
  tmp_dir = "tmp." + time.strftime("%Y%m%d%H%M%S", time.localtime())
    
  #check and delete tmp dir if exists
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
  tar = tarfile.open(sour_file)
  names=tar.getnames()

  for f_name in names:
    if (f_name.find('.') >= 0) and (f_name[f_name.rindex('.'):len(f_name)] == '.svm'):  #only process target ".svm" data
      i = len(day_list)
      while i:
        if ("Day" + day_list[i-1] +".svm") == f_name[f_name.rindex("/")+1:] :
          print("Processing file " +tmp_dir+"/"+f_name+" ...")
          tar.extract(f_name, path = tmp_dir)
          procRawfile(output, tmp_dir+"/"+f_name, dimension)
          break
        i = i - 1
    else:
      print("Non-target file"+tmp_dir+"/"+f_name)
      

  tar.close()
  output.write("\.")
  output.flush()
  output.close()

  #delete tmp file
  shutil.rmtree(tmp_dir)


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile, dimension):
  
  global g_count,attr_L,label_L,id_L,line_limit

  input = open(rawfile)
  while True:
    line = input.readline()
    if not line: break
    L = line.split(" ")

    if len(attr_L) >= line_limit:
      #write result
      i = 0
      while i < len(attr_L):
        output.write("%s#{%s ,%s}#%s\\n\n" % (id_L[i], 1, str(attr_L[i])[1:-1], label_L[i]) )
        i += 1
        
      attr_L = []
      label_L = []
      id_L = []
    else:
      #continue produce data
      #proc id
      id_L.append(g_count)
      #print("Debug: processing line "+str(g_count))

      #proc L[0] as label
      y = 1
      if L[0].strip() == "-1": y = 0
      label_L.append(y)
      
      
      #proc L[1 - other] as vector x      
      tmp_L = [0] * dimension      
      i = len(L) -1
      while i:
          tmp_L[int(L[i].strip().split(":")[0])] = float(L[i].strip().split(":")[1].strip())
          i = i-1
        
      attr_L.append(tmp_L)
      g_count = g_count + 1

  if 0 != len(attr_L):
    i = 0
    while i < len(attr_L):
      output.write("%s#{%s ,%s}#%s\\n\n" % (id_L[i], 1, str(attr_L[i])[1:-1], label_L[i]) )
      i += 1
        
    attr_L = []
    label_L = []
    id_L = []

  input.close()

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
