#!/usr/bin/env python
import sys, os, zipfile, shutil

g_count = 0      #global count to generate id
dict_L = []      #global Dict List


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
  
  tmp_dir = "tmp." + options.destfile 

  #check tmp dir
  if os.path.isdir(tmp_dir): shutil.rmtree(tmp_dir)


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


  #Initial Dict List
  i = 0
  while i < 7:
      if len(dict_L) < (i+1):dict_L.append({})
      i = i + 1


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
  global g_count
  global dict_L

  input = open(rawfile,"r")
  while True:
    line = input.readline()
    if not line: break                  #when end, then break
    L = line.strip().split(",")

    if 7 != len(L): continue            #jump bad data
    
    #proc id
    output.write(str(g_count) + "#{")
    g_count = g_count + 1

    #proc [0~5] as attr
    i = 0
    while i < 6:
        D = dict_L[i]
        if D.has_key(L[i].strip()): output.write(str(D.get(L[i].strip()))+",")
        else:
            k = len(D)                      #get target dict length
            D[L[i].strip()] = k + 1         #add new element to target dict
            output.write(str(D.get(L[i].strip()))+",")
        i = i + 1

    #proc [6] as class
    output.seek(-1,1)
    D = dict_L[i]
    if D.has_key(L[i].strip()): output.write("}#"+str(D.get(L[i].strip()))+"\\n\n")
    else:
        k = len(D)                          #get target dict length
        D[L[i].strip()] = k + 1             #add new element to target dict
        output.write("}#"+str(D.get(L[i].strip()))+"\\n\n")

  input.close()

  

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
