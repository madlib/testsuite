#!/usr/bin/env python
import sys, os

g_count = 0      #global count to generate id



# =======================
# === Function : main ===
# =======================
def main():
  from optparse import OptionParser
  
  #optparser
  usage = "usage: %prog [options] arg"
  parser = OptionParser(usage)
  parser.add_option("-s", "--sourfile1", action="store", dest="sourfile1", type="string")
  parser.add_option("-S", "--sourfile2", action="store", dest="sourfile2", type="string")
  parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  (options, args) = parser.parse_args()

  #variable def
  sour_file1 = options.sourfile1       #source file 1
  sour_file2 = options.sourfile2       #source file 2
  dest_file = options.destfile          #where target sql file stores
  tb_name = options.tablename           #get table name
  

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


  #processing source data
  print("Processing file: " + sour_file1)
  procRawfile(output, sour_file1)

  print("Processing file: " + sour_file2)
  procRawfile(output, sour_file2)

      

  output.write("\. \n")
  output.flush()
  output.close()
  
  


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, rawfile):
  global g_count

  input = open(rawfile,"r")
  while True:
    line = input.readline()
    if not line: break                  #when end, then break
    L = line.strip().split(",")

    if 11 != len(L): break

    tmp_L = []
    tmp_L.append(1)
    for i in range(10):
        tmp_L.append(float(L[i]))
        i += 1
      
    output.write("%s#{%s}#%s\\n\n" % (g_count, str(tmp_L)[1:-1], str(int(L[10]))))
    g_count += 1
    
  input.close()

  

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
