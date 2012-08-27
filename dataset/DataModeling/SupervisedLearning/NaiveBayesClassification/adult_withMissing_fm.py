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


  #Initial Dict List
  i = 0
  while i < 15:
      if len(dict_L) < (i+1):dict_L.append({})
      i = i + 1
      

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

    if 15 != len(L): continue            #jump bad data
    
    #proc id
    output.write(str(g_count) + "#{")
    g_count = g_count + 1


    #proc [0~13] as attr
    i = 0
    while i < 14:
        if 0 == i:
            #age
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                m = int(L[i].strip())
                if 0 <= m and m <20:
                    output.write("1,")
                elif m > 80:
                    output.write("8,")
                else:
                    output.write(str(m/10)+",")
        elif 2 == i:
            #fnlwgt
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                m = int(L[i].strip())
                if 0 <= m and m < 40000:
                    output.write("1,")
                elif m > 400000:
                    output.write("20,")
                else:
                    output.write(str(m / 20000)+",")
        elif 4 == i:
            #education_num
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                m = int(L[i].strip())
                if m <=6:
                    output.write("1,")
                elif m > 6 and m <= 8:
                    output.write("2,")
                elif m > 8 and m <= 12:
                    output.write("3,")
                else:
                    output.write("4,")
        elif 10 == i:
            #capital_gain
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                m = int(L[i].strip())
                if 0 == m:output.write("0,")
                else:
                    m = m / 5000
                    if m > 1:
                      output.write("3,")
                    else:
                      output.write(str(m + 1)+",")
        elif 11 == i:
            #capital_loss
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                m = int(L[i].strip())
                if 0 == m: output.write("0,")
                else:
                    m = int(L[i].strip()) / 250
                    if m < 6:
                      output.write("1,")
                    elif m > 9:
                      output.write("5,")
                    else:
                      output.write(str(m - 5)+",")
        elif 12 == i:
            #hours per week
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                m = int(L[i].strip())
                if m < 24:
                    output.write("1,")
                elif m >=24 and m < 32:
                    output.write("2,")
                elif m >=32 and m < 40:
                    output.write("3,")
                else:
                    output.write("4,")
        else:
            #use dict_L to process
            if "?" == L[i].strip(): output.write("NULL,")
            else:
                D = dict_L[i]
                if D.has_key(L[i].strip()): output.write(str(D.get(L[i].strip()))+",")
                else:
                    k = len(D)                      #get target dict length
                    D[L[i].strip()] = k + 1         #add new element to target dict
                    output.write(str(D.get(L[i].strip()))+",")
        i = i + 1

    #proc [14] as class
    output.seek(-1,1)
    if "?" == L[i].strip(): print("no class error!")
    else:
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
