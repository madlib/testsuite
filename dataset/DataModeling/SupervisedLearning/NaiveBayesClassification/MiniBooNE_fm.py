#!/usr/bin/env python
import sys, os, shutil


# =======================
# === Function : main ===
# =======================
def main():
  from optparse import OptionParser
  
  #optparser
  usage = "usage: %prog [options] arg"
  parser = OptionParser(usage)
  parser.add_option("-s", "--sourfile", action="store", dest="sourfile", type="string", metavar="file")
  parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  (options, args) = parser.parse_args()


  #variable def
  sour_file = options.sourfile          #the source file list
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

  g_count = 0   #data id

  min_L = [99999]*50
  max_L = [-99999]*50
  share_L = [0]*50
  
  #parse source data
  print("Processing file"+ sour_file)
  
  input = open(sour_file,"r")

  while True:
    line = input.readline()
    if not line:break                 #end of line

    if len(line) !=701:continue       #incorrect format

    j = 0
    while j < 700:
      m = float(line[j: (j+14)])
      if m > -999:                    #Skip singular value
        if m < min_L[j/14]:
          min_L[j/14] = m
        if m > max_L[j/14]:
          max_L[j/14] = m
        
      j += 14

  i = 0
  while i<50:
    share_L[i] = (max_L[i] - min_L[i])/10
    i += 1

  #proc data
  input.seek(0)  
  L = input.readline().strip().split(" ")
  #processing +1 class
  k = int(L[0].strip())
  i = 0
  while i < k:
      line = input.readline()
      i = i + 1
      if not line:
          print("Error 01!")
          return
        
      if len(line) != 701 : continue
      
      id = g_count
      g_count = g_count + 1


      tmp_L=[]
      j = 0
      while j < 700:
        m = float(line[j: (j+14)])
        if m > -999:
          if m < (min_L[j/14] + 1 * share_L[j/14]):
            tmp_L.append(1)
          elif m < (min_L[j/14] + 2 * share_L[j/14]):
            tmp_L.append(2)
          elif m < (min_L[j/14] + 3 * share_L[j/14]):
            tmp_L.append(3)
          elif m < (min_L[j/14] + 4 * share_L[j/14]):
            tmp_L.append(4)
          elif m < (min_L[j/14] + 5 * share_L[j/14]):
            tmp_L.append(5)
          elif m < (min_L[j/14] + 6 * share_L[j/14]):
            tmp_L.append(6)
          elif m < (min_L[j/14] + 7 * share_L[j/14]):
            tmp_L.append(7)
          elif m < (min_L[j/14] + 8 * share_L[j/14]):
            tmp_L.append(8)
          elif m < (min_L[j/14] + 9 * share_L[j/14]):
            tmp_L.append(9)
          else:
            tmp_L.append(10)
        else:
          tmp_L.append(0)

        j += 14

      output.write("%s#{%s}#%s\\n\n" % (id, str(tmp_L)[1:-1], 1))
          

  #processing -1 class
  k = int(L[1].strip())
  i = 0
  while i < k:
      line = input.readline()
      i = i + 1
      if not line:
          print("Error 01!")
          return
        
      if len(line) != 701 : continue
      
      id = g_count
      g_count = g_count + 1


      tmp_L=[]
      j = 0
      while j < 700:
        m = float(line[j: (j+14)])
        if m > -999:
          if m < (min_L[j/14] + 1 * share_L[j/14]):
            tmp_L.append(1)
          elif m < (min_L[j/14] + 2 * share_L[j/14]):
            tmp_L.append(2)
          elif m < (min_L[j/14] + 3 * share_L[j/14]):
            tmp_L.append(3)
          elif m < (min_L[j/14] + 4 * share_L[j/14]):
            tmp_L.append(4)
          elif m < (min_L[j/14] + 5 * share_L[j/14]):
            tmp_L.append(5)
          elif m < (min_L[j/14] + 6 * share_L[j/14]):
            tmp_L.append(6)
          elif m < (min_L[j/14] + 7 * share_L[j/14]):
            tmp_L.append(7)
          elif m < (min_L[j/14] + 8 * share_L[j/14]):
            tmp_L.append(8)
          elif m < (min_L[j/14] + 9 * share_L[j/14]):
            tmp_L.append(9)
          else:
            tmp_L.append(10)
        else:
          tmp_L.append(0)

        j += 14

      output.write("%s#{%s}#%s\\n\n" % (id, str(tmp_L)[1:-1], -1))
      

  output.write("\.\n")
  output.write("alter table "+tb_name+" owner to madlibtester;\n")
  output.flush()
  output.close()
  

# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
