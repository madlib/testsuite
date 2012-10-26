#!/usr/bin/env python
import sys, os, zipfile, shutil

g_count = 0      #global count to generate id


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
  sour_file = options.sourfile          #the source file
  dest_file = options.destfile   #where target sql file stores
  tb_name = options.tablename           #get table name
  
  tmp_dir = "tmp." + os.path.split(options.destfile)[1]

  #check tmp dir
  if os.path.isdir(tmp_dir): shutil.rmtree(tmp_dir)


  #create dest_data file
  if os.path.isfile(dest_file):os.remove(dest_file)
  output = open(dest_file,"w")

  output.write("--check table\n");
  output.write("SET client_min_messages TO WARNING;DROP TABLE IF EXISTS "+ tb_name +" CASCADE;\n");
  output.write("\n");
  output.write("--create table\n");
  output.write("create table "+ tb_name +"(pid bigint, position double precision[]);\n");
  output.write("\n");
  output.write("COPY "+ tb_name +" from stdin delimiter '#';\n");

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
    procRawfile(output, t_file)

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
  global g_count

  avg_L = [0]*5408
  sum_L = [0]*5408
  sum2_L = [0]*5408
  d_L = [0]*5408
  c = 0

  L1=[0]*5408
  L2=[0]*5408
  L3=[0]*5408
  L4=[0]*5408

  #compute sum
  input = open(rawfile,"r")
  while True:
    line = input.readline()
    if not line: break
    if line.find("?") >=0 : continue
    
    L = line.strip().split(",")
    if 5410 != len(L): continue
    
    i = 0
    while i < 5408:
      try:
        sum_L[i] += float(L[i])
      except:
        print(line)
        print(c)
        return        
      i += 1

    c += 1

  #compute avg
  i = 0
  while i < 5408:
    avg_L[i] = sum_L[i] / c
    i += 1

  #compute d
  input.seek(0)
  while True:
    line = input.readline()
    if not line: break
    if line.find("?") >=0 : continue
    
    L = line.strip().split(",")
    if 5410 != len(L): continue

    i = 0
    while i < 5408:
      sum2_L[i] += (float(L[i]) - avg_L[i]) ** 2
      i += 1

  #compute d, u-3d, u-d, u+d, u+3d
  i = 0
  while i < 5408:
    d_L[i] = (sum2_L[i] / c)*(0.5)
    L1[i] = avg_L[i] - 3*d_L[i]
    L2[i] = avg_L[i] - d_L[i]
    L3[i] = avg_L[i] + d_L[i]
    L4[i] = avg_L[i] + 3*d_L[i]
    
    i += 1

  #
  input.seek(0)
  while True:
    line = input.readline()
    if not line: break
    if line.find("?") >=0 : continue
    L = line.strip().split(",")
    
    #proc id
    id = g_count
    g_count=g_count+1

    #proc L[0~5407]
    tmp_L=[]
    i = 0
    while i < 5408:
      m = float(L[i])
      if m < L1[i]:
        tmp_L.append(1)
      elif m < L2[i]:
        tmp_L.append(2)
      elif m < L3[i]:
        tmp_L.append(3)
      elif m < L4[i]:
        tmp_L.append(4)
      else:
        tmp_L.append(5)
        
      i=i+1

    #proc L[5408]
    
    output.write("%s#{%s} \n" % (id, str(tmp_L)[1:-1]))

  input.close()



# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
