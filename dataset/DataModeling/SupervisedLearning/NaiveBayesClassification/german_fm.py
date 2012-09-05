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

  input = open(rawfile,"r")
  test_L = [False, True, False, False, True, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False]  
  while True:
    line = input.readline()
    if not line: break
    L = line.strip().split(" ")
    
    #proc id
    output.write(str(g_count) + "#{")
    g_count = g_count + 1

    #proc [0~19]
    i = 0
    while i < 20:
        if test_L[i]:   #additional process
            if i == 1:
                output.write(str((int(L[i].strip()) + 1) / 12) + ",")
            elif i == 4:
                m = int(L[i].strip())
                if 0 <= m and m < 4000: output.write(str(m/500)+",")            #class 0 ~ 7
                elif 4000 <= m and m < 10000: output.write(str(m/1000 + 4)+",") #class 8 ~ 13
                else: output.write("14,")                                       #class 14
            elif i == 12:
                m = int(L[i].strip())       
                if 0 <= m and m < 20: output.write("1,")    #class 1
                elif m >= 80: output.write("8,")            #class 8
                else: output.write(str(m/10)+",")           #class 2 ~ 7
            else:
                print("Error!")
                print(str(g_count) + " : " + str(i) + " : " + line)
                return
        else:           #output directly
            if L[i][0] == "A": output.write(L[i][1:len(L[i])]+",")
            else: output.write(L[i]+",")
        i = i + 1
        
    #proc [20]
    output.seek(-1,1)
    output.write("}#"+L[i].strip()+"\\n\n")

  input.close()

  

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
