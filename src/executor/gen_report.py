#!/usr/bin/env python

import os, sys

sys.path.append('../')
from utility import run_sql

class ReportGenerator:
    def __init__(self, psql_args, schema, run_id, platform, ReportDir ):
        self.psql_args = psql_args
        self.schema = schema
        self.run_id = run_id
        self.platform = platform
        self.ReportDir = ReportDir
        self.CWD = os.getcwd()
    
    def gen_summaryreport(self):
        summaryreport_sql = "SELECT 'Test Suite Name|' || suitename || '|Test Case Name|' || casename || '|Test Detail|' || testresult || ' (' || elapsedtime || '.00 ms)' || '|Test Status|' || testresult AS MADlib_Test_Report FROM %s.summaryreport where runid = %s ;"%(self.schema,self.run_id)
        summaryreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_summary.report' 
        copyToFile(self.psql_args, summaryreport_sql, os.path.join(self.CWD, summaryreport_filename))
        
        PerformanceSummaryreport_sql = "SELECT suitename, perfstatus, count AS Number FROM %s.performancesummary;"% self.schema
        PerformanceSummaryreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_PerformanceSummary.report'
        copyToFile(self.psql_args, PerformanceSummaryreport_sql, os.path.join(self.CWD, PerformanceSummaryreport_filename))

        FeaturetestSummaryreport_sql = "SELECT suitename, testresult, count AS number FROM %s.featuretestsummary ORDER BY suitename, testresult;"% self.schema
        FeaturetestSummaryreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_FeaturetestSummary.report'
        copyToFile(self.psql_args, FeaturetestSummaryreport_sql, os.path.join(self.CWD, FeaturetestSummaryreport_filename))

    def gen_skippedcasesreport(self):
        skippedcasesreport_sql = "SELECT  *  FROM %s.skippedcases ORDER BY fixversion DESC, algorithmic;"% self.schema
        skippedcasesreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_skippedcases.report'
        copyToFile(self.psql_args, skippedcasesreport_sql, os.path.join(self.CWD, skippedcasesreport_filename))
    
    def gen_failedcasesreport(self):
        FailedCasesreport_sql = "SELECT DISTINCT casename FROM %s.failedcases;"% self.schema
        FailedCasesreport_filename = self.ReportDir + self.platform + '_' + self.run_id + '_FailedCases.report'
        copyToFile(self.psql_args, FailedCasesreport_sql, os.path.join(self.CWD, FailedCasesreport_filename))

        dirpath = self.ReportDir + self.platform + '_' + self.run_id + "Failedcases_report" + '/'
        os.system('rm -rf ' + dirpath)
        os.system('mkdir -p ' + dirpath)
        sql="select %s.gen_failedreport( '%s', '%s')"%(self.schema, dirpath, self.schema)
        try:
            run_sql.runSQL(sql,  psqlArgs = self.psql_args)
        except Exception,exp:
            print  exp
            print '\nError when generating failed cases reports'

def copyToFile(psql_args, sql, filename):
    """generate test result report
    
    param sql as select, but we need to create as temp table to store 
        those data and copy to file 
    """
    out = run_sql.runSQL(sql, psqlArgs = psql_args)
    f = open(filename, 'w')
    f.write(out)
    f.close()

def generate_report(psql_args, schema, run_id, platform, ReportDir = '../../report/' ):
    generator = ReportGenerator(psql_args, schema, run_id, platform, ReportDir )
    generator.gen_summaryreport()
    generator.gen_skippedcasesreport()
    generator.gen_failedcasesreport()

def main():
    psql_args= ['-U', 'gpdbchina', '-h', 'localhost', '-p', '5466', '-d', 'benchreport']
    ret = run_sql.runSQL("select max(runid) from benchmark.testitemseq;", psqlArgs = psql_args)
    run_id = ret.splitlines()[2].strip()
    platform = "TEST"
    schema = "benchmark"
    generate_report(psql_args, schema, run_id, platform)

if __name__ == '__main__':
    main()
