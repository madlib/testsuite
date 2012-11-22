#!/usr/bin/env python
"""This is the entry script of this project. 

Most functions start with 'python run.py [-i][-g][-s]'"""
import glob, os
import sys, subprocess

sys.path.append('../src/')

from  utility.argparse import ArgumentParser
from  utility import test_config, analytics_tool, file_path, run_sql, tools
from  executor import gen_report, run_case
from  loader.loadingManager import loadingManager

Path = file_path.Path()

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

    parser.add_argument("-l", "--forceload", action='store_true', help = "Reconvert and reload all tables set in config.yaml and tables.yaml.")
    parser.add_argument("-L", "--smartload", action='store_true', help = "Load modules by config.yaml, tables.yaml and -m. If table exists in db, do nothing.")
    parser.add_argument("-m", "--module", nargs = "*", help = "Modules selected to load. For exapme: 'run.py -Lm Dec Ran' means only load decisition tree and random forest.")

    parser.add_argument("-s", "--schedule", help = "Set schedule file and run.")

    options = parser.parse_args()

    testConfiger = test_config.Configer(Path.TestConfigXml)
    testConfiger.testconfig()
    analyticsTools = analytics_tool.AnalyticsTools(Path.AnalyticsToolXml)
    analyticsTools.parseTools()

    psql_args = testConfiger.getResultDBmanager().getDBsqlArgs()
    schema   = testConfiger.getResultSchema()
    tools.set_search_path(schema, Path.BootstrapDir)

    if options.schedule:
        map_file = options.schedule
        plans    = tools.parserMap(Path.ScheduleDir + map_file)

        run_sql.runSQL("update %s.testitemseq set runid = runid + 1;"%schema, psqlArgs = psql_args)
        ret = run_sql.runSQL("select max(runid) from %s.testitemseq;"%schema, psqlArgs = psql_args)
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
            
            version = run_case.runCases(filename, plan['skip'], isList, isUnique, plan['platform'], analyticsTools, testConfiger, run_id)
            if plan['skip']:
                run_sql.runSQL(Path.BootstrapDir + 'skipsqlfile.sql', psqlArgs = psql_args, isFile = True)
            run_sql.runSQL(Path.BootstrapDir + 'post.sql', psqlArgs = psql_args, onErrorStop = False, isFile = True)
            
            gen_report.generate_report(psql_args, schema, run_id, plan['platform'], Path.ReportDir)

    #load data set to all databases to test
    if options.forceload:
        loading_manager = loadingManager(Path.RootPath, 'madlibtestdata', analyticsTools)
        loading_manager.do(options.module, False, True, True)

    if options.initbenchmark:
        #initialization
        run_sql.runSQL(Path.BootstrapDir + 'init.sql', psqlArgs = psql_args, isFile = True)
        run_sql.runSQL(Path.BootstrapDir + 'init_cases.sql', psqlArgs = psql_args, isFile = True)
        run_sql.runSQL(Path.BootstrapDir + 'resultbaseline.sql', psqlArgs = psql_args, isFile = True)
        #generate new cases
        os.system('cd ../src/generator/ && python ./gen_testcase.py')
        run_sql.runSQL(Path.BootstrapDir + 'analyticstool.sql', psqlArgs = psql_args, isFile = True)
        #initialize algorithm result table
        run_sql.runSQL(Path.BootstrapDir + 'algorithmspec.sql', psqlArgs = psql_args, isFile = True)
        for sqlfile in glob.glob('../testcase/*.sql'):
            run_sql.runSQL(sqlfile, psqlArgs = psql_args, onErrorStop = False, isFile = True)
    if options.gencase:
        #initialization
        run_sql.runSQL(Path.BootstrapDir + 'init_cases.sql', psqlArgs = psql_args, isFile = True)
        run_sql.runSQL(Path.BootstrapDir + 'resultbaseline.sql', psqlArgs = psql_args, isFile = True)
        #generate new cases
        if options.debug:
            os.system('cd ../src/generator/ && python ./gen_testcase.py debug')
        else:
            os.system('cd ../src/generator/ && python ./gen_testcase.py')
        run_sql.runSQL(Path.BootstrapDir + 'analyticstool.sql', psqlArgs = psql_args, isFile = True)
        run_sql.runSQL(Path.BootstrapDir + 'algorithmspec.sql', psqlArgs = psql_args, onErrorStop = False,  isFile = True)
        for sqlfile in glob.glob('../testcase/*.sql'):
            run_sql.runSQL(sqlfile, psqlArgs = psql_args, onErrorStop = False, isFile = True)

    if options.smartload:
        loading_manager = loadingManager(Path.RootPath, 'madlibtestdata', analyticsTools)
        loading_manager.do(options.module, False, False, False)

if __name__ == '__main__':
    main()

