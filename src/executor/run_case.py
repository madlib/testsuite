#!/usr/bin/env python
"""This file specialize the way to run cases."""

import sys, os
import read_skipfiles, execute_case

sys.path.append('../')
from utility import file_path,  run_sql

Path = file_path.Path()

def __getList(filename):
    """Parse file and return a list of non-empty lines."""
    return [ l.strip() for l in open(filename).readlines() if l.strip() != '' and l.strip()[0] != '#']

def __getCasesFromSingle(filename):
    """read cases from casesfile and return them."""
    filepath = Path.ScheduleDir + filename
    if filepath is None or not os.path.isfile(filepath):
        sys.exit('ERROR: Can not find the schedule file.')
    try:
        cases = __getList(filepath)
    except IOError:
        sys.exit('ERROR: Open schedule file failed.')
    for case in cases:
        if not os.path.isfile(Path.TestCaseDir + case + '.case'):
            sys.exit('ERROR: Case file: %s missing.' % case)
    return cases

def __getCasesFromMulti(filename):
    """Read cases form list file and teturn them."""
    filepath = Path.ScheduleDir + filename
    if filepath is None or not os.path.isfile(filepath):
        sys.exit('ERROR: Listfile missing.')
    try:
        files =  __getList(filepath)
        cases = []
        for file in files:
            cases += __getCasesFromSingle(file)
    except IOError:
        sys.exit('ERROR: Open listfile failed.')
    return cases

def __distinctingCases(cases):
    """remove duplicate cases form input cases."""
    distinctcases = []
    for case in cases:
        try:
            distinctcases.index(case)
        except ValueError:
            distinctcases.append(case)
    return distinctcases

def __skipCases(cases, skipfilename, testConfiger):
    """Skip cases and return remainning cases.
    
    @param cases: cases to be skipped.
    @param skipfilename:skip file name.
    """
    skipfilepath = Path.ScheduleDir + skipfilename
    skip_sqlfilepath = Path.BootstrapDir +'skipsqlfile.sql'
    if cases is None:
        sys.exit('ERROR: Cases missing.')
    if skipfilepath is None or not os.path.isfile(skipfilepath):
        sys.exit('ERROR: Skip file missing.')

    run_cases = []
    skippedcases = []
    try:
        skipfile_reader = read_skipfiles.ReadSkipfiles(skipfilepath, skip_sqlfilepath, testConfiger.resultDBSchema)
        skips = skipfile_reader.getNoRunCases()
        for case in cases:
            try:
                skips.index(case)
                skippedcases.append(case)
            except ValueError:
                run_cases.append(case)
    except IOError:
        sys.exit('ERROR: Skip file open failed.')
    except Exception,exp:
        print exp
        sys.exit ('Error when parsing skip_files')

    return (run_cases, skippedcases)

def __parserCasesFromFile(getfile, skipfile, isList, isUnique, testConfiger):
    """Parse cases and return them.
    @param getfile: filename store in map file.
    @param skipfile: skip filename store in map file.
    @param isList: is the filename a cases file or a lists file.
    @pram isUnique: if true do remove duplicate cases form result.
    @pram testConfiger: result DB configer.
    """
    skippedcases = []
    if isList:
        cases = __getCasesFromMulti(getfile)
    else:
        cases = __getCasesFromSingle(getfile)
    if isUnique:
        cases = __distinctingCases(cases)
    if skipfile:
        (cases,skippedcases) = __skipCases(cases, skipfile, testConfiger)
    return (cases,skippedcases)

def runCases(getfile, skipfile, isList, isUnique, platform, analyticsTool, testConfiger, run_id):
    """run the cases specialized by getfile. """
    (cases, skippedcases) = __parserCasesFromFile(getfile, skipfile, isList, isUnique, testConfiger)

    if not platform in analyticsTool.analyticsTools:
        sys.exit('ERROR: Wrong platform name.')
    testdbconf = analyticsTool.analyticsTools[platform] 
    executor = execute_case.TestCaseExecutor(cases, testdbconf, platform)

    executor.executeCase(Path.TestCaseDir, run_id)
    
    if skipfile:
        for case in skippedcases:
                f = open( Path.TestCaseDir +case+'.case')
                lines = f.readlines()

                for line in lines:
                    if len(line) < 10:
                        continue
                    pos =line.find('target_base_name')
                    if pos > 0:
                        target_base_name = line[pos + 17:].strip()
                    else:
                        continue
                    print target_base_name
                    pos = line.find('-c')
                    cmd = line[pos + 3:].strip()
                    sql = """insert into %s.testitemresult
                        values( '%s', %s, %s, '%s',
                        '%s', %s, %s, '%s', '%s', %s::bool);
                        """ % (testConfiger.getResultSchema(), target_base_name, run_id, \
                            0, 'table', platform, 0,
                            'NULL', 'NULL', cmd, False)

                    result = run_sql.runSQL(sql, testConfiger.user, None, testConfiger.host, testConfiger.port, testConfiger.database,['--expanded'])

    return executor.version

