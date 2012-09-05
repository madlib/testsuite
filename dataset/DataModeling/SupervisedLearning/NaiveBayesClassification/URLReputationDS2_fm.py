#!/usr/bin/env python
import sys, os, tarfile, shutil

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
  parser.add_option("-s", "--sourdata", action="store", dest="sourdata", type="string")
  parser.add_option("-d", "--destdata", action="store", dest="destdata", type="string")
  parser.add_option("-D", "--dimension", action="store", dest="dimension", type="int")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  parser.add_option("-w", "--daylist", action="store", dest="daylist", type="string")
  (options, args) = parser.parse_args()


  #variable def
  sour_data = options.sourdata          #the source file
  dest_data = options.destdata   #where target sql file stores
  dimension = options.dimension         #get dimension for attributes
  tb_name = options.tablename           #get table name
  day_list = options.daylist.strip().split(",")
  
  tmp_dir = "tmp." + os.path.split(options.destdata)[1]   #extract and generate tmp data


  #check and delete tmp dir if exists
  if os.path.isdir(tmp_dir): shutil.rmtree(tmp_dir)


  #create dest_data file
  if os.path.isfile(dest_data):os.remove(dest_data)
  output = open(dest_data,"w")

  output.write("--check table\n");
  output.write("SET client_min_messages TO WARNING;DROP TABLE IF EXISTS "+ tb_name +" CASCADE;\n");
  output.write("\n");
  output.write("--create table\n");
  output.write("create table "+ tb_name +"(id int, attributes int[], class int);\n");
  output.write("\n");
  output.write("COPY "+ tb_name +" from stdin delimiter '#';\n");

  
  #parse source data
  tar = tarfile.open(sour_data)
  names=tar.getnames()
  """
  print("Getting dimension ...")
  for f_name in names:
    tar.extract(f_name, path = tmp_dir)
    if (f_name.find('.') >= 0) and (f_name[f_name.rindex('.'):len(f_name)] == '.svm'):  #only extract target ".gz" data
      d = getDimension(tmp_dir+"/"+f_name)
      if d > dimension: dimension = d
  """      
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
  output.write("\.\n")
  output.write("alter table "+tb_name+" owner to madlibtester;\n")
  output.flush()
  output.close()

  #delete tmp file
  shutil.rmtree(tmp_dir)

# ===============================
# === Function : getDimension ===
# ===============================
"""
def getDimension(file):
  d=0
  input = open(file,"r")
  while True:
    line = input.readline()
    if not line: break
    L = line.split(" ")
    k = int(L[len(L)-1].split(":")[0])
    if d > k : d = k

  input.close()
  return d
"""


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
        output.write("%s#{%s}#%s\\n\n" % (id_L[i], str(attr_L[i])[1:-1], label_L[i]) )
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
      if L[0].strip() == "-1": y = -1
      label_L.append(y)
      
      
      #proc L[1 - other] as vector x      
      tmp_L = [0] * dimension      
      i = len(L) -1
      while i:
        if float(L[i].strip().split(":")[1].strip()) > 0.5: tmp_L[int(L[i].strip().split(":")[0])] = 1 
        i = i-1
        
      attr_L.append(tmp_L)
      g_count = g_count + 1

  if 0 != len(attr_L):
    i = 0
    while i < len(attr_L):
      output.write("%s#{%s}#%s\\n\n" % (id_L[i], str(attr_L[i])[1:-1], label_L[i]) )
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
