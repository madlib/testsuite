#!/usr/bin/env python
"""This is the entry script of this project. 

Most functions start with 'python run.py [-i][-g][-s]'"""
import glob
import os
import sys
from datetime import datetime
from optparse import OptionParser

import utility
 
TestCaseDir     = '../testcase/'
TestMataDir     = '../bootstrap/'
GeneratorSrcDir = '../src/generator/'
AnalyticsTool   = '../testspec/metadata/analyticstool.xml'
ReportDir       = '../report/'
ScheduleDir     = '../schedule/'

def getList(filename):
    """Parse file and return a list of non-empty lines"""
    return [ l.strip() for l in open(filename).readlines() if l.strip() != '' and l.strip()[0] != '#']

def generateReport(psql_cmd, environ, run_id, platform):
    """generate test result report
    
    Select * from benchmark.testreport where runid = {run_id};  
    Select * from benchmark.detailtestreport where runid = {run_id};  
    """
    CWD = os.getcwd()
    date_str = datetime.today().strftime('%Y%m%d%H%M%S')
    testreport_sql = "select * from benchmark.testreport where runid = %s ;" % run_id
    testreport_filename = ReportDir + platform + '_' + date_str + '_' + run_id + '_test.report'
    copyToFile(psql_cmd, environ, testreport_sql, os.path.join(CWD, testreport_filename))  
    detailreport_sql = "select * from benchmark.detailtestreport where runid = %s ;" % run_id
    detailreport_filename = ReportDir + platform + '_' + date_str + '_' + run_id + '_detail.report'
    copyToFile(psql_cmd, environ, detailreport_sql, os.path.join(CWD, detailreport_filename))

def copyToFile(psql_cmd, environ, sql, filename):
    """generate test result report
    
    Select * from benchmark.testreport where runid = {run_id};
    Select * from benchmark.detailtestreport where runid = {run_id};
    param sql as select, but we need to create as temp table to store 
        those data and copy to file 
    """
    out = utility.runSQL(psql_cmd, sql, environ, False)
    f = open(filename, 'w')
    f.write(out)

def main():
    """Change the options, del -c -S -t -r. Modify -s. i.e. python run.py -s map.yaml"""
    sys.path.append(GeneratorSrcDir)
    use = """usage: ./run.py --loaddata --gencase --init 
                --schedule map_file
                --genreport run_id
        --init or -i for short, to clean up and init logger database
        --schedule or -s for short, to load the schedule file and run
        --loaddata or -l for short, to load data or not
        --gencase  or -g for short, to generate test cases
        --genreport or -G for short, to generate test report by run_id
    """
    parser = OptionParser(usage=use)
    parser.add_option("-i", "--init"        , action="store_true"   , default = False)
    parser.add_option("-G", "--genreport"   , action="store"        , type="string")
    parser.add_option("-g", "--gencase"     , action="store_true"   , dest="gencase"    , default = False)
    parser.add_option("-l", "--loaddata"    , action="store_true"   , dest="loaddata"   , default = False)
    parser.add_option("-s", "--schedule"         , action="store"        , type="string"     , dest="schedule")
    (options, _) = parser.parse_args()

    if not options.schedule and not options.genreport and not options.init and not options.loaddata and not options.gencase:
        print use
   
    psql_cmd = utility.getResultDBPsqlCMD()
    environ = os.environ
    import execute_case
    if options.schedule:
        map_file = options.schedule
        plans = utility.parserMap(map_file)
        utility.runSQL(psql_cmd, "update benchmark.testitemseq set runid = runid + 1;"
                           , environ, False)
        ret = utility.runSQL(psql_cmd, "select max(runid) from benchmark.testitemseq;"
                                , environ, False)
        run_id = ret.splitlines()[2].strip()
        for plan in plans:
            if len(plan) > 4:
                sys.exit('ERROR:-s arg file has some grammer error, too many lines.')
            if not 'skip' in plan:
                plan['skip'] = ""
            if 'cases' in plan:
                isList = False
                filename = plan['cases']
            if 'lists' in plan:
                isList = True
                filename = plan['lists']
            if 'unique' in plan and plan['unique']:
                isUnique = True
            else:
                isUnique = False
            cases = utility.parserCasesFromFile(filename, plan['skip'], isList, isUnique)
            #key method to send sql to database.
            executor = execute_case.TestCaseExecutor(cases, plan['platform'])
            executor.executeCase(TestCaseDir, AnalyticsTool, run_id)
            utility.runSQL(psql_cmd, TestMataDir + 'post.sql', environ)
            generateReport(psql_cmd, environ, run_id, plan['platform'])
            version = executor.version
            #version = executor.madlibVersion(plan['platform'])
            insert_testinfo_sql = "insert into benchmark.testinfo (runid,cases_count,platform,madlib_version) values('"+\
                run_id+"','"+str(len(cases))+"','"+plan['platform']+"','"+version+"');"
            utility.runSQL(psql_cmd, insert_testinfo_sql , environ, False)

    #load data set to all databases to test
    if options.loaddata:
        os.system('python ./load.py')
    if options.init:
        #initialization
        utility.runSQL(psql_cmd, TestMataDir + 'init.sql', environ)
        utility.runSQL(psql_cmd, TestMataDir + 'init_cases.sql', environ)
        utility.runSQL(psql_cmd, TestMataDir + 'resultbaseline.sql', environ)
        #generate new cases
        os.system('cd ../src/generator/ && python ./gen_testcase.py')
        utility.runSQL(psql_cmd, TestMataDir + 'analyticstool.sql', environ)
        #initialize algorithm result table
        utility.runSQL(psql_cmd, TestMataDir + 'algorithmspec.sql', environ)
        for sqlfile in glob.glob('../testcase/*.sql'):
            utility.runSQL(psql_cmd, sqlfile, environ)
    if options.gencase:
        #initialization
        utility.runSQL(psql_cmd, TestMataDir + 'init_cases.sql', environ)
        #generate new cases
        os.system('cd ../src/generator/ && python ./gen_testcase.py')
        utility.runSQL(psql_cmd, TestMataDir + '/analyticstool.sql', environ)
        for sqlfile in glob.glob('../testcase/*.sql'):
            utility.runSQL(psql_cmd, sqlfile, environ)
    if options.genreport:
        generateReport(psql_cmd, environ, options.genreport, 'all')

if __name__ == '__main__':
    main()

