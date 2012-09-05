#!/usr/bin/env python
import sys, os, shutil

g_count = 0      #global count to generate id


# =======================
# === Function : main ===
# =======================
def main():
  from optparse import OptionParser
  
  #optparser
  usage = "usage: %prog [options] arg"
  parser = OptionParser(usage)
  parser.add_option("-s", "--sourfilelist", action="store", dest="sourfilelist", type="string", help="source file separated by ','", metavar="file1,file2")
  parser.add_option("-l", "--labelfilelist", action="store", dest="labelfilelist", type="string", help="label file separated by ',' and one source file should have one correspondence label file ", metavar="label1,label2")
  parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
  parser.add_option("-D", "--dimension", action="store", dest="dimension", type="int")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  (options, args) = parser.parse_args()


  #variable def
  sour_file_list = options.sourfilelist.split(",")          #the source file list
  label_file_list = options.labelfilelist.split(",")        #the labed file list

  num=len(sour_file_list)
  if num != len(label_file_list):
    print("Error: unmatched sour file number with labe file number")
    return
  
  dest_data = options.destfile                       #where target sql file stores
  dimension = options.dimension                             #get dimension for attributes
  tb_name = options.tablename                               #get table name
  

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
  while num:
    print("Processing file"+ sour_file_list[num-1] +" and "+ label_file_list[num-1])
    procRawfile(output, sour_file_list[num-1], label_file_list[num-1], dimension)
    num = num - 1

  output.write("\.\n")
  output.write("alter table "+tb_name+" owner to madlibtester;\n")
  output.flush()
  output.close()
  


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, sour_file, label_file, dimension):
  global g_count

  input_sour = open(sour_file, "r")
  input_label = open(label_file, "r")
  while True:
    line_sour = input_sour.readline().strip()
    line_label = input_label.readline().strip()
    if ((not line_sour) or (not line_label)): break
    L = line_sour.split(" ")

    #proc id
    output.write(str(g_count) + "#{" )
    g_count = g_count + 1
    
    #proc L_sour as x
    length = len(L)
    i = 0
    d = 0
    
    while d < dimension:
      if (i < length) and (int(L[i].strip()) == (d + 1)):
        output.write("1,")
        i = i + 1
      else:
        output.write("0,")
      d = d + 1
        
    #proc line_label as y
    output.seek(-1,1)
    if line_label.strip() == "1": output.write("}#1\\n\n")
    else: output.write("}#-1\\n\n")

  input_sour.close()
  input_label.close()



# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
