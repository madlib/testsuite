#!/usr/bin/env python
# Filename gen_report.py

import os
import subprocess
import time
import sys

def runSQL(cmd, sql, environ, onErrorStop = True):
    """Run sql in database.
    """
    psql_cmd = cmd + ['-c', sql]
    psqlProcess = subprocess.Popen(psql_cmd, env = environ, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = None)
    (stdoutdata, _) = psqlProcess.communicate()
    return stdoutdata

def copyToFile(psql_cmd, environ, sql, filename):
    """generate test result report
    param sql as select, but we need to create as temp table to store 
        those data and copy to file 
     """    
    out = runSQL(psql_cmd, sql, environ )    
    f = open(filename, 'w')
    f.write(out)
    f.close()

class ReportGenerator:
    def __init__(self, psql_cmd, environ, run_id, platform, ReportDir ):
        self.psql_cmd = psql_cmd
        self.environ = environ
        self.run_id = run_id
        self.platform = platform
        self.ReportDir = ReportDir
        self.CWD = os.getcwd()
    
    def gen_summaryreport(self):
        summaryreport_sql = "SELECT 'Test Suite Name|' || suitename || '|Test Case Name|' || casename || '|Test Detail|' || testresult || ' (' || elapsedtime || '.00 ms)' || '|Test Status|' || testresult AS MADlib_Test_Report FROM benchmark.summaryreport where runid = %s ;" % self.run_id
        summaryreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_summary.report' 
        copyToFile(self.psql_cmd, self.environ, summaryreport_sql, os.path.join(self.CWD, summaryreport_filename))
        
        PerfermanceSummaryreport_sql = "SELECT suitename, perfstatus, count AS Number FROM benchmark.perfermancesummary;"
        PerfermanceSummaryreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_PerfermanceSummary.report'
        copyToFile(self.psql_cmd, self.environ, PerfermanceSummaryreport_sql, os.path.join(self.CWD, PerfermanceSummaryreport_filename))

        FeaturetestSummaryreport_sql = "SELECT suitename, testresult_summary, count AS number FROM benchmark.featuretestsummary ORDER BY suitename, testresult_summary;"
        FeaturetestSummaryreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_FeaturetestSummary.report'
        copyToFile(self.psql_cmd, self.environ, FeaturetestSummaryreport_sql, os.path.join(self.CWD, FeaturetestSummaryreport_filename))

    def gen_skippedcasesreport(self):
        skippedcasesreport_sql = "SELECT  *  FROM benchmark.skippedcases ORDER BY fixversion DESC;"
        skippedcasesreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_skippedcases.report'
        copyToFile(self.psql_cmd, self.environ, skippedcasesreport_sql, os.path.join(self.CWD, skippedcasesreport_filename))
    
    def gen_failedcasesreport(self):
        FailedCasesreport_sql = "SELECT DISTINCT casename FROM benchmark.failedcases;"
        FailedCasesreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_FailedCases.report'
        copyToFile(self.psql_cmd, self.environ, FailedCasesreport_sql, os.path.join(self.CWD, FailedCasesreport_filename))

        dirpath = self.ReportDir + self.platform + '_' + self.run_id + "Failedcases_report" + '/'
        os.system('rm -rf ' + dirpath)
        os.system('mkdir -p ' + dirpath)
        dirpath = os.path.abspath(dirpath) + '/' 
        sql="select benchmark.gen_failedreport( '%s', 'benchmark')"%dirpath 
        try:
            runSQL(self.psql_cmd, sql, self.environ)
        except Exception,exp:
            print  exp
            print '\nError when generating failed cases reports'

def generate_report(psql_cmd, environ, run_id, platform, ReportDir = '../../report/' ):
    generator = ReportGenerator(psql_cmd, environ, run_id, platform, ReportDir )
    generator.gen_summaryreport()
    generator.gen_skippedcasesreport()
    generator.gen_failedcasesreport()

def main():
    environ = os.environ
    psql_cmd= ['psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-U', 'gpdbchina', '-h', 'localhost', '-p', '5466', '-d', 'benchmark']
    ret = runSQL(psql_cmd, "select max(runid) from benchmark.testitemseq;", environ, False)
    run_id = ret.splitlines()[2].strip()
    platform = ""
    generate_report(psql_cmd, environ, run_id, platform)

if __name__ == '__main__':
    main()
