#!/usr/bin/env python
import sys, os, shutil

g_count = 0         #global count to generate id
write_limit = 10    #write limit

id_w_L = []         #id write cache
y_w_L = []          #y write cache
attr_w_L = []       #attr write cache


# =======================
# === Function : main ===
# =======================
def main():
  from optparse import OptionParser
  
  #optparser
  usage = "usage: %prog [options] arg"
  parser = OptionParser(usage)
  parser.add_option("-s", "--sourfilelist", action="store", dest="sourfilelist", type="string", help="input source file separated by ','")
  parser.add_option("-d", "--destfile", action="store", dest="destfile", type="string", help="output sql file")
  parser.add_option("-D", "--dimension", action="store", dest="dimension", type="int")
  parser.add_option("-t", "--tablename", action="store", dest="tablename", type="string")
  (options, args) = parser.parse_args()


  #variable def
  sour_file_list = options.sourfilelist.split(",")          #the source file list

  num=len(sour_file_list)
  
  dest_data = options.destfile                       #where target sql file stores
  dimension = options.dimension                             #get dimension
  tb_name = options.tablename                               #get table name


  #create dest_data file
  if os.path.isfile(dest_data):os.remove(dest_data)
  output = open(dest_data,"w")

  output.write("--check table\n");
  output.write("SET client_min_messages TO WARNING;DROP TABLE IF EXISTS "+ tb_name +" CASCADE;\n");
  output.write("\n");
  output.write("--create table\n");
  output.write("create table "+ tb_name +"(id int, ind float8[], label float8);\n");
  output.write("\n");
  output.write("copy "+ tb_name +" from stdin delimiter '#';\n");

  
  #parse source data
  
  while num:
      print("Processing file: "+ sour_file_list[num-1])
      #check file type
      if (sour_file_list[num-1].find('.') >= 0) and (sour_file_list[num-1][sour_file_list[num-1].rindex('.'):] == '.bz2'):
          #check whether exists, if not exists then unzip
          flag = 0
          L = os.path.split(sour_file_list[num-1])
          fileName = L[1][:L[1].rindex(".")]
          
          if L[0].strip() == "":
            dirList = os.listdir("./")
            for content in dirList:
              if fileName == content:
                flag = 1
          else:
            dirList = os.listdir(L[0])
            for content in dirList:
              if fileName == content:
                flag = 1

          if flag == 0: #continue uncompressing
            print("Uncompressing file"+ sour_file_list[num-1])
            os.system("cp "+sour_file_list[num-1] +" "+sour_file_list[num-1]+"_bak")
            os.system("bzip2 -d "+sour_file_list[num-1])

          if L[0].strip() == "":
            procRawfile(output, fileName, dimension)  #process
            os.remove(fileName)                       #delete
          else:
            procRawfile(output, L[0]+"/"+fileName, dimension)
            os.remove(L[0]+"/"+fileName)

          #mv bak back
          if flag == 0:
            os.system("mv "+sour_file_list[num-1]+"_bak "+sour_file_list[num-1])
          
      else: #process directly
          procRawfile(output, sour_file_list[num-1], dimension)
      num = num - 1

  output.write("\.\n")
  output.write("alter table "+tb_name+" owner to madlibtester;\n")
  output.flush()
  output.close()
  
  print("Finished!")


# ==============================
# === Function : procRawfile ===
# ==============================
def procRawfile(output, sour_file, dimension):
    
  global g_count,write_limit,id_w_L,y_w_L,attr_w_L

  input= open(sour_file, "r")
  while True:
      line = input.readline()
      if not line: break
      L = line.strip().split(" ")

      #print("Debug: line "+str(g_count))
      if len(L) < 2:
          print("pass line " + str(g_count) + ": " + line)
          continue

      #process y
      y_w_L.append(L[0].strip())
      

      #process attrs
      tmp_L = [0] * dimension      
      
      attr_L=line[len(L[0]):].strip().split(" ")
      i = len(attr_L)
      if i > dimension:
          print("Error: smaller dimension!") #we believe data is well formatted
          return

      j = 0
      while j < i:
          try:
              tmp_L[int(attr_L[j].split(":")[0].strip())-1] = float(attr_L[j].split(":")[1].strip())
          except:
              print(str(j) + ":" + str(i))
              return
          j = j+1
      attr_w_L.append(tmp_L)

      #process id
      id_w_L.append(g_count)
      g_count += 1

      #check whether write out
      if len(y_w_L) >= write_limit:
          i = 0
          while i < write_limit:
              output.write("%s#{%s}#%s\\n\n" % (id_w_L[i], str(attr_w_L[i])[1:-1], y_w_L[i]))
              i += 1
          id_w_L = []
          attr_w_L = []
          y_w_L = []

  #check if there still have values
  if len(y_w_L) != 0 :
      i = 0
      k = len(y_w_L)
      while i < k:
          output.write("%s#{%s}#%s\\n\n" % (id_w_L[i], str(attr_w_L[i])[1:-1], y_w_L[i]))
          i += 1
      id_w_L = []
      attr_w_L = []
      y_w_L = []

  input.close()


# ========================
# === Function : entry ===
# ========================
if __name__ == "__main__":
  main()
