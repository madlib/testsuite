#!/usr/bin/env python
"""This is the entry script of this project. 

Most functions start with 'python run.py [-i][-g][-s]'"""
import glob
import os
import sys, subprocess
from datetime import datetime
from optparse import OptionParser


TestCaseDir     = '../testcase/'
TestMataDir     = '../bootstrap/'
GeneratorSrcDir = '../src/generator/'
AnalyticsTool   = '../testspec/metadata/analyticstool.xml'
ReportDir       = '../report/'
ScheduleDir     = '../schedule/'
SrcDir          = '../src/'
TestConfig      = '../testspec/metadata/testconfig.xml'

sys.path.append(SrcDir)
from generator.argparse import ArgumentParser
from generator.test_config import Configer

test_cfg = Configer(TestConfig)
test_cfg.testconfig()

from loader.loadingManager import loadingManager
import utility


def getList(filename):
    """Parse file and return a list of non-empty lines"""
    return [ l.strip() for l in open(filename).readlines() if l.strip() != '' and l.strip()[0] != '#']

def generateReport(psql_cmd, environ, run_id, platform):
    """generate test result report
    
    """
    CWD = os.getcwd()
    date_str = datetime.today().strftime('%Y%m%d%H%M%S')

    summaryreport_sql = "SELECT 'Test Suite Name|' || suitename || '|Test Case Name|' || itemname || '|Test Detail|' || testresult || ' - ' || perf_status || ' (' || elapsedtime || '.00 ms)' || '|Test Status|' || testresult AS MADlib_Test_Report FROM benchmark.summaryreport where runid = %s ;" % run_id
    summaryreport_filename = ReportDir + platform + '_' + run_id + '_summary.report'
    copyToFile(psql_cmd, environ, summaryreport_sql, os.path.join(CWD, summaryreport_filename))
     
    FailedCasesreport_sql = "SELECT DISTINCT casename FROM benchmark.failedcases;" 
    FailedCasesreport_filename = ReportDir + platform + '_' + run_id + '_FailedCases.report' 
    copyToFile(psql_cmd, environ, FailedCasesreport_sql, os.path.join(CWD, FailedCasesreport_filename))

    PerfermanceSummaryreport_sql = "SELECT suitename, perfstatus, count AS Number FROM benchmark.perfermancesummary;"
    PerfermanceSummaryreport_filename = ReportDir + platform + '_' + run_id + '_PerfermanceSummary.report'
    copyToFile(psql_cmd, environ, PerfermanceSummaryreport_sql, os.path.join(CWD, PerfermanceSummaryreport_filename))

    FeaturetestSummaryreport_sql = "SELECT suitename, testresult_summary, count AS number FROM benchmark.featuretestsummary ORDER BY suitename, testresult_summary;"
    FeaturetestSummaryreport_filename = ReportDir + platform + '_' + run_id + '_FeaturetestSummary.report'
    copyToFile(psql_cmd, environ, FeaturetestSummaryreport_sql, os.path.join(CWD, FeaturetestSummaryreport_filename))

    skippedcasesreport_sql = "SELECT  *  FROM benchmark.skippedcases ORDER BY fixversion DESC;"
    skippedcasesreport_filename = ReportDir + platform + '_' + run_id + '_skippedcases.report' 
    copyToFile(psql_cmd, environ, skippedcasesreport_sql, os.path.join(CWD, skippedcasesreport_filename))

def copyToFile(psql_cmd, environ, sql, filename):
    """generate test result report
    
    param sql as select, but we need to create as temp table to store 
        those data and copy to file 
    """
    out = utility.runSQL(psql_cmd, sql, environ, False)
    f = open(filename, 'w')
    f.write(out)

def main():
    """Change the options, del -c -S -t -r. Modify -s. i.e. python run.py -s map.yaml"""
    use = """usage: ./run.py --loaddata --gencase --init 
                --schedule map_file
                --genreport run_id
        --init or -i for short, to clean up and init logger database
        --schedule or -s for short, to load the schedule file and run
        --loaddata or -l for short, to load data or not
        --gencase  or -g for short, to generate test cases
        --genreport or -G for short, to generate test report by run_id
    """
    parser = ArgumentParser(description=use)
    parser.add_argument("-i", "--initbenchmark", action='store_true', help = "Initial benchmark db.")

    parser.add_argument("-g", "--gencase", action='store_true', help = "Generate cases.")
    parser.add_argument("-d", "--debug", action = 'store_true', help ="Debug model will generate all sql for each case, it take a long time.")

    parser.add_argument("-l", "--forceload", action='store_true', help = "Drop db, reconvert and reload all tables set in config.yaml and tables.yaml.")
    parser.add_argument("-L", "--smartload", action='store_true', help = "Load modules by config.yaml, tables.yaml and -m. If table exists in db, do nothing.")
    parser.add_argument("-m", "--module", nargs = "*", help = "Modules selected to load. For exapme: 'run.py -Lm Dec Ran' means only load decisition tree and random forest.")

    parser.add_argument("-s", "--schedule", help = "Set schedule file and run.")

    options = parser.parse_args()

    psql_cmd = utility.getResultDBPsqlCMD()
    environ = os.environ
    import generator.execute_case
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

            version = utility.runCases(filename, plan['skip'], isList, isUnique, plan['platform'], TestCaseDir, AnalyticsTool, run_id)
            if plan['skip']:
                utility.runSQL(psql_cmd, TestMataDir + 'skipsqlfile.sql', environ)
            
            utility.runSQL(psql_cmd, TestMataDir + 'post.sql', environ)
            generateReport(psql_cmd, environ, run_id, plan['platform'])

    #load data set to all databases to test
    if options.forceload:
        loading_manager = loadingManager('..', 'madlibtestdata')
        loading_manager.do(options.module, False, True, True)

    if options.initbenchmark:
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
        utility.runSQL(psql_cmd, TestMataDir + 'resultbaseline.sql', environ)
        #generate new cases
        if options.debug:
            os.system('cd ../src/generator/ && python ./gen_testcase.py debug')
        else:
            os.system('cd ../src/generator/ && python ./gen_testcase.py')
        utility.runSQL(psql_cmd, TestMataDir + '/analyticstool.sql', environ)
        psql_cmd = utility.getResultDBPsqlCMD(onErrorStop = False)
        print psql_cmd
        utility.runSQL(psql_cmd, TestMataDir + 'algorithmspec.sql', environ, onErrorStop = False)

        for sqlfile in glob.glob('../testcase/*.sql'):
            utility.runSQL(psql_cmd, sqlfile, environ)

    if options.smartload:
        loading_manager = loadingManager('..', 'madlibtestdata')
        loading_manager.do(options.module, False, False, False)

if __name__ == '__main__':
    main()

