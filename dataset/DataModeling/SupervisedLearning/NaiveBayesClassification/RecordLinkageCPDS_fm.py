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
  parser.add_option("-s", "--sourdata", action="store", dest="sourdata", type="string")
  parser.add_option("-d", "--destdata", action="store", dest="destdata", type="string")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  (options, args) = parser.parse_args()


  #variable def
  sour_data = options.sourdata          #the source file
  dest_data = options.destdata   #where target sql file stores
  tb_name = options.tablename           #get table name
  
  tmp_dir = "tmp." + os.path.split(options.destdata)[1]

  #check tmp dir
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
  if zipfile.is_zipfile(sour_data):
    z = zipfile.ZipFile(sour_data, 'r')
    for f in z.namelist():
      if (f.find('.') >= 0) and (f[f.rindex('.'):len(f)] == '.zip'):  #only extract target ".zip" data
        zfile = z.extract(f,path = tmp_dir)
        zz = zipfile.ZipFile(zfile, 'r')
        for ff in zz.namelist():
          zzz = zz.extract(ff,path=tmp_dir)
          # zzz is the raw data, call fun to do this
          print("Processing file " +zzz+" ...")
          procRawfile(output, zzz)
      else:
        print("Non-target file: "+f)

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

  input = open(rawfile)
  input.readline()
  while True:
    line = input.readline()
    if not line: break
    L = line.strip().split(",")
    
    #proc L[0~1] as id
    output.write(str(g_count) + "#{")
    g_count=g_count+1

    #proc L[2~10]
    i = 2
    while i < 11:
      if(L[i].strip() == '?'):output.write("NULL,") #proc missing value
      else:
        if 0.5 < float(L[i].strip()): output.write("2,")
        else: output.write("1,")
      i=i+1

    #proc L[11]
    output.seek(-1,1)
    if("TRUE" == L[i].strip()):output.write("}#1\\n\n")
    elif("FALSE" == L[i].strip()):output.write("}#2\\n\n")
    else:output.write("}#1\\n\n")

  input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
