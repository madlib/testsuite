#!/usr/bin/env python
import sys, os, gzip

g_count = 0      #global count to generate id

u_L=[2959,155,14,269,46,2350,212,223,142,1980]
d_L=[279.9839281,111.9151464,7.483314774,212.5488179,58.29236657,1559.253347,26.75817632,19.74841766,38.27531842,1324.194095]
L1=[]
L2=[]
L3=[]
L4=[]


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
  output.write("create table "+ tb_name +"(id int, attributes int[], class int);\n");
  output.write("\n");
  output.write("COPY "+ tb_name +" from stdin delimiter '#';\n");

  #initial data
  global u_L,d_L,L1,L2,L3,L4

  length = len(u_L)
  if length != len(d_L):
      print("Error: expectation num unequal to standard deviation!")
      return
  i = 0
  while i < length:
      L1.append(int(u_L[i] - 3 * d_L[i]))
      L2.append(int(u_L[i] - d_L[i]))
      L3.append(int(u_L[i] + d_L[i]))
      L4.append(int(u_L[i] + 3 * d_L[i]))
      i += 1

  #parse source data
  print("Processing data: "+ sour_file)
  procRawfile(output, sour_file)

  output.write("\.\n")
  output.write("alter table "+tb_name+" owner to madlibtester;\n")
  output.flush()
  output.close()
  
  


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
  global g_count,L1,L2,L3,L4

  input = open(rawfile,"r")
  while True:
    line = input.readline()
    if not line: break
    L = line.strip().split(",")
    
    #proc id
    output.write(str(g_count) + "#{")
    g_count = g_count + 1

    #proc [0~9] as attr part1
    i = 0
    while i < 10:
        if "?" == L[i].strip(): output.write("NULL,")
        else:
            m = int(L[i].strip())
            if m < L1[i]:output.write("1,")
            elif m < L2[i]:output.write("2,")
            elif m < L3[i]:output.write("3,")
            elif m < L4[i]:output.write("4,")
            else : output.write("5,")
        i += 1
        
    #proc [10-53] as attr part2 + part3
    while i < 54:
        if "?" == L[i].strip(): output.write("NULL,")
        else: output.write(L[i].strip()+",")
        i += 1

    #proc [54] as class
    output.seek(-1,1)
    if "?" == L[i].strip():
        print("Error: no tar class!")
        output.write("}#NULL\\n\n")
        continue
    output.write("}#"+L[i].strip()+"\\n\n")

  input.close()

  

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
