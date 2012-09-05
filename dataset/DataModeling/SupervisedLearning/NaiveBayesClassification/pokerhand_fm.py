#!/usr/bin/env python
import sys, os

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
  parser.add_option("-s", "--sourfilelist", action="store", dest="sourfilelist", type="string",help="input the file name, seperated by ','")
  parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  (options, args) = parser.parse_args()

  #variable def
  sour_file_list = options.sourfilelist.strip().split(",")          #the source file
  dest_file = options.destfile   #where target sql file stores
  tb_name = options.tablename           #get table name
  

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
  k = 0
  while k < len(sour_file_list):
      print("Processing file: " + sour_file_list[k])
      procRawfile(output, sour_file_list[k])
      k += 1

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

    if 11 != len(L): break

    #process id
    output.write(str(g_count)+"#{")
    g_count = g_count + 1

    #process L[0~9] as attr
    i = 0
    while i < 10:
        output.write(L[i].strip()+",")
        i += 1

    #process L[10] as class
    output.seek(-1,1)
    output.write("}#"+L[i]+"\\n\n")

    
  input.close()

  

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
