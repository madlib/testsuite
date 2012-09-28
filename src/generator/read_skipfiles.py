#!/usr/bin/env python
# Filename read_skipfiles.py
'''
   read skip files and update the db
'''
import glob
import os
import sys

from file_path import *

class ReadSkipfiles:
    '''skip files reader.'''

    def __init__(self, skipfilepath = "../../schedule/skip_knownissues",sqlfilePath = "../../bootstrap/skipsqlfile.sql"):
        '''
        skipfilepath: skip file path
        sqlfilePath:  file path of sql file, which will  create the Madlib_Jiras table and jiras_cases table
        '''
        self.skipfilepath = skipfilepath
        self.sqlfilePath = sqlfilePath 
        self.skipfiles  = [self.skipfilepath] 
        self.jiras_list = []
        self.mapping_list = []
        self.noRunCaselist =[]
        self.Jiras_table = "benchmark.Madlib_Jiras"
        self.Mapping_table = "benchmark.jiras_cases"
  
    def getNoRunCases(self):
        '''read the skip files and return the cases to be skipped.'''
        self.gen_jiraInfolists()
        self.gen_sqlfile()
        return self.noRunCaselist
    
    def gen_jiraInfolists(self):
        ''' handle the skip files, and record the data in jiras_list, mapping_list '''
        for skipfile in self.skipfiles:
            fp = open(skipfile)
            lines_count = 0
            line = fp.readline();lines_count = lines_count + 1
            while line:
                if line.strip('# \n').upper() == "HEAD":
                    line = fp.readline();lines_count = lines_count + 1
                    jiraId = ""
                    jiraType = ""
                    jiraDes = ""
                    while line and line.strip('# \n').upper() != "END":
                        if line.startswith('#'):
                            if line.strip('# \n').upper().startswith("JIRAID:"):
                                if jiraId != "":
                                    print '\n File: "%s",\n Redundant JiraId in line  %s\n'%(skipfile,lines_count)
                                    sys.exit()
                                else:
                                    jiraId = line.strip('# \n')[len("JIRAID:"):].strip()
                            elif line.strip('# \n').upper().startswith("TYPE:"):
                                jiraType = line.strip('# \n')[len("TYPE:"):].strip()
                            elif line.strip('# \n').upper().startswith("DES:"):
                                jiraDes = line.strip('# \n')[len("DES:"):].strip()
                            elif line.strip('# \n').upper().startswith("HEAD:"):
                                print '\n File: "%s",\n Expected a END tag in line  %s\n'%(skipfile,lines_count)
                                sys.exit()
                        elif jiraId == "":
                            print '\n File: "%s",\n Expected a JiraId in line  %s\n'%(skipfile,lines_count)
                            sys.exit()
                        elif line.strip('# \n') != "":
                            if int(jiraType) != 0:
                                self.noRunCaselist.append( (line.strip('# \n') ) )
                            self.mapping_list.append( (jiraId,line.strip('# \n') ) )
                        line = fp.readline();lines_count = lines_count + 1
                    self.jiras_list.append( ( "'" + jiraId + "'", jiraType, "'"+jiraDes+"'") )
                line = fp.readline();lines_count = lines_count + 1
            fp.close()

    def gen_sqlfile(self):
        '''generate sql file, which will be run in run.py'''
        skipcases_sql = open( self.sqlfilePath,"w")
        jira_tableAttrs = [ "JiraId" , "JiraType", "JiraDescription"]
        mapping_tableAttrs = ["JiraId" , "CaseName"]
        
        stmt = "DELETE FROM " + self.Jiras_table + ";\nDELETE FROM " + self.Mapping_table + ";\n\n"
        skipcases_sql.write(stmt)

        # fomat the insert stmt
        for (jiraId,jiraType,jiraDes) in self.jiras_list:
            if jiraType == "":
                jiraType = "NULL"
            if jiraDes == "''":
                jiraDes = "NULL"
            stmt = "INSERT INTO " + self.Jiras_table + "\n("+ ",".join(jira_tableAttrs) + ")\n" + \
               "VALUES" + "(" + jiraId + "," + jiraType + "," + jiraDes +  ");\n\n"
            skipcases_sql.write(stmt)
        skipcases_sql.write("\n\n")

        for(jiraId,caseName) in self.mapping_list:
            stmt = "INSERT INTO " + self.Mapping_table + "\n("+ ",".join(mapping_tableAttrs) + ")\n" + \
               "VALUES" + "('" +jiraId + "', '" + caseName + "');\n\n"
            skipcases_sql.write(stmt)

        skipcases_sql.close()   

    def main(self):
        self.gen_jiraInfolists()
        self.gen_sqlfile()
        print  self.noRunCaselist

if __name__ == '__main__':
     reader = ReadSkipfiles()
     reader.main()
