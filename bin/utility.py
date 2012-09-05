#!/usr/bin/env python
"""This file provide functions for run.py to read file, parse cases and run sql."""
from xml.dom.minidom import parse, parseString
import os, sys
import subprocess
import socket
import yaml

RootDir, _ = os.path.split(os.getcwd()+'/'+sys.argv[0])
os.chdir(RootDir)

TestConfigXml = '../testspec/metadata/testconfig.xml'
AnalyticsTool = '../testspec/metadata/analyticstool.xml'
PostgresPsql  = '/usr/local/pgsql/bin/psql'
ScheduleDir   = '../schedule/'
CaseDir       = '../testcase/'
def getList(filename):
    """Parse file and return a list of non-empty lines."""
    return [ l.strip() for l in open(filename).readlines() if l.strip() != '' and l.strip()[0] != '#']

def parserMap(mapfile):
    """Parse the map file and return a list that contains plans."""
    mapfile = ScheduleDir + mapfile
    if mapfile is None or not os.path.isfile(mapfile):
        sys.exit('ERROR: Can not find the map file. Check whether the file is in schedule folder.')
    try:
        f = open(mapfile)
        plans = yaml.load(f)
        f.close()
    except IOError:
        sys.exit('ERROR: Open map file failed.')
    return plans

def __getCasesFromSingle(filename):
    """read cases from casesfile and return them."""
    filepath = ScheduleDir + filename
    if filepath is None or not os.path.isfile(filepath):
        sys.exit('ERROR: Can not find the schedule file.')
    try:
        cases = getList(filepath)
    except IOError:
        sys.exit('ERROR: Open schedule file failed.')
    for case in cases:
        if not os.path.isfile(CaseDir + case + '.case'):
            sys.exit('ERROR: Case file: %s missing.' % case)
    return cases

def __getCasesFromMulti(filename):
    """Read cases form list file and teturn them."""
    filepath = ScheduleDir + filename
    if filepath is None or not os.path.isfile(filepath):
        sys.exit('ERROR: Listfile missing.')
    try:
        files =  getList(filepath)
        cases = []
        for file in files:
            cases += __getCasesFromSingle(file)
    except IOError:
        sys.exit('ERROR: Open listfile failed.')
    return cases

def __skipCases(cases, skipfilename):
    """Skip cases and return remainning cases.
    
    @param cases: cases to be skipped.
    @param skipfilename:skip file name.
    """
    skipfilepath = ScheduleDir + skipfilename
    if cases is None:
        sys.exit('ERROR: Cases missing.')
    if skipfilepath is None or not os.path.isfile(skipfilepath):
        sys.exit('ERROR: Skip file missing.')
    try:
        skips = getList(skipfilepath) 
        skippedcases = []
        for case in cases:
            try:
                skips.index(case)
            except ValueError:
                skippedcases.append(case)
    except IOError:
        sys.exit('ERROR: Skip file open failed.')
    return skippedcases

GeneratorSrcDir = '../src/generator/'

def runCases(getfile, skipfile, isList, isUnique, platform, testCaseDir, analyticsTool, run_id):

    sys.path.append(GeneratorSrcDir)
    import execute_case
    import test_config
    import file_path
    import run_sql
    import template_executor

    configer = test_config.Configer('../testspec/metadata/' + file_path.Path.testconfigXml)
    configer.testconfig()

    if isList:
        cases = __getCasesFromMulti(getfile)
    else:
        cases = __getCasesFromSingle(getfile)
    if isUnique:
        cases = __distinctingCases(cases)

    executor = execute_case.TestCaseExecutor(cases, platform)

    if skipfile:
        skipfilepath = ScheduleDir + skipfile
        if cases is None:
            sys.exit('ERROR: Cases missing.')
        if skipfilepath is None or not os.path.isfile(skipfilepath):
            sys.exit('ERROR: Skip file missing.')

        try:
            skips = getList(skipfilepath)
        except IOError:
            sys.exit('ERROR: Skip file open failed.')

        executor.executeStart(analyticsTool)

        skippedcases = []
        for case in cases:
            try:
                skips.index(case)

                f = open('../testcase/'+case+'.case')
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
                        """ % (configer.metaDBSchema, target_base_name, run_id, \
                            0, 'table', platform, 0,
                            'NULL', 'NULL', cmd, False)
     
                    result = run_sql.runSQL(sql, configer.user, None, configer.host, configer.port, configer.database,['--expanded'])
            except ValueError:
                executor.executeOneCase(testCaseDir, platform, case, run_id)
            
        executor.executeStop()
    else:
        executor.executeCase(testCaseDir, analyticsTool, run_id)
    return executor.version


def parserCasesFromFile(getfile, skipfile, isList, isUnique):
    """Parse cases and return them.
    @param getfile: filename store in map file.
    @param skipfile: skip filename store in map file.
    @param isList: is the filename a cases file or a lists file.
    @pram isUnique: if true do remove duplicate cases form result.
    """
    if isList:
        cases = __getCasesFromMulti(getfile)
    else:
        cases = __getCasesFromSingle(getfile)
    if skipfile:
        cases = __skipCases(cases, skipfile)
    if isUnique:
        cases = __distinctingCases(cases)
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

def getResultDBConnection():
    """Return the result database connection."""
    try :
        XmlDoc = parse(TestConfigXml)
    except Exception:
        print "Xml \" %s\" Format is Invalid !" % (TestConfigXml)
        raise

    configuration = XmlDoc.getElementsByTagName("configuration")[0]

    # parse meta database info
    metadatadb = configuration.getElementsByTagName("metadatadb")[0]
    if metadatadb.getElementsByTagName("user"):
        user = metadatadb.getElementsByTagName("user")[0].childNodes[0].data.strip().encode('ASCII')
    if metadatadb.getElementsByTagName("host"):
        host = metadatadb.getElementsByTagName("host")[0].childNodes[0].data.strip().encode('ASCII')
    if metadatadb.getElementsByTagName("port"):
        port = metadatadb.getElementsByTagName("port")[0].childNodes[0].data.strip().encode('ASCII')
    if metadatadb.getElementsByTagName("database"):
        database = metadatadb.getElementsByTagName("database")[0].childNodes[0].data.strip().encode('ASCII')
    return user + '@' + host + ':' + port + '/' + database

def getResultDBPsqlCMD(onErrorStop = True):
    """Return the result database psql command."""
    testConfig = open(TestConfigXml)
    if onErrorStop is True:
        psql_cmd = ['psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1']
    else:
        psql_cmd = ['psql', '-X', '-q', '-v', 'ON_ERROR_STOP=off']

    environ = dict(os.environ)
    try :
        XmlDoc = parse(testConfig)
    except Exception:
        print "Xml \" %s\" Format is Invalid !" % (TestConfigXml)
        raise

    configuration = XmlDoc.getElementsByTagName("configuration")[0]

    # parse meta database info
    metadatadb = configuration.getElementsByTagName("metadatadb")[0]
    if metadatadb.getElementsByTagName("user"):
        user = metadatadb.getElementsByTagName("user")[0].childNodes[0].data.strip().encode('ASCII')
        psql_cmd.extend(['-U', user])
    if metadatadb.getElementsByTagName("passwd"):
        passwd = metadatadb.getElementsByTagName("passwd")[0].childNodes[0].data.strip().encode('ASCII')
        environ['PGPASSWORD'] = passwd
    if metadatadb.getElementsByTagName("host"):
        host = metadatadb.getElementsByTagName("host")[0].childNodes[0].data.strip().encode('ASCII')
        psql_cmd.extend(['-h', host])
    if metadatadb.getElementsByTagName("port"):
        port = metadatadb.getElementsByTagName("port")[0].childNodes[0].data.strip().encode('ASCII')
        psql_cmd.extend(['-p', port])
    if metadatadb.getElementsByTagName("database"):
        database = metadatadb.getElementsByTagName("database")[0].childNodes[0].data.strip().encode('ASCII')
        psql_cmd.extend(['-d', database])
    if metadatadb.getElementsByTagName("psql"):
        psql = metadatadb.getElementsByTagName("psql")[0].childNodes[0].data.strip().encode('ASCII')
        psql_cmd[0] = psql

    return psql_cmd

def runSQL(cmd, sql, environ, isFile = True, onErrorStop = True):
    """Run sql in database.
    param isFile: if true deal with sql as a *.sql file.
    """
    if isFile:
        psql_cmd = cmd + ['-f', sql]
    else:
        psql_cmd = cmd + ['-c', sql]

    psqlProcess = subprocess.Popen(psql_cmd, env = environ, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = None)
    (stdoutdata, _) = psqlProcess.communicate()
    return stdoutdata


class Parser:
    """Class that parse xml file."""
    def __init__(self, fileName):
        try :
            self.fileName = fileName
            self.xmlDoc = parse(fileName)
        except Exception:
            print "Xml \"%s\" Format is Invalid !" % (self.fileName)
            raise

    def getNodeTag(self, node, name):
        """get node tag by node name."""
        return node.getElementsByTagName( name )[0]

    def getNodeVal(self, node, name):
        """get node value by node name"""
        return node.getElementsByTagName(name)[0].childNodes[0].data.strip().encode('ASCII')

    def getNodeList(self, node, name):
        """get node list by node name"""
        return node.getElementsByTagName( name )


class AnalyticsTools(Parser):
    """Class that parses analyticsTolls.xml file."""
    def __init__(self, fileName):
        Parser.__init__(self, fileName)
        self.analyticsTools = {}

    def parserTools(self):
        """Parse xml file and save data to self.analyticsTools"""
        analytics_tools = Parser.getNodeTag(self, self.xmlDoc,"analytics_tools")

        atList = Parser.getNodeList(self, analytics_tools, "analytics_tool")
        atDic = {}
        for at in atList:
            name = Parser.getNodeVal(self, at, "name")
            kind = Parser.getNodeVal(self, at, "kind").upper()

            if kind == "GREENPLUM" or kind == "POSTGRES":
                atDic["name"] = Parser.getNodeVal(self, at,"name")
                atDic["kind"]  = Parser.getNodeVal(self, at,"kind")
                atDic["toolversion"] = Parser.getNodeVal(self, at,"toolversion")
                atDic["madlibversion"] = Parser.getNodeVal(self, at,"madlibversion")
                atDic["host"] = Parser.getNodeVal(self, at,"host")
                atDic["port"] = Parser.getNodeVal(self, at,"port")
                atDic["database"] = Parser.getNodeVal(self, at,"database")
                atDic["username"] = Parser.getNodeVal(self, at,"user")
                atDic["superusername"] = Parser.getNodeVal(self, at,"superuser")
                atDic["segmentnum"] = Parser.getNodeVal(self, at,"segmentnum")
                atDic["master_dir"] = Parser.getNodeVal(self, at,"master_dir")
                atDic["sql_source"] = Parser.getNodeVal(self, at,"sql_source")
                atDic["env"] = Parser.getNodeVal(self, at,"env")
            # unsupported or invalid analytical tools such as R and MAHOUT, etc
            else:
                sys.exit( '%s is not a supported analytical tool!' % (kind) )

            self.analyticsTools[name] = atDic
            atDic = {}

class AddUser :
    def __init__(self, username):
        self.user_name = username
        self.pghbastrs = []
        self.pghbastrs.extend( ["local    all  %s     trust"%self.user_name] )
        self.pghbastrs.extend( ["host    all  %s     %s/28   trust"%(self.user_name, ip) 
                                                        for ip in getIpv4Addr() ] )

    def run_sql_stmts(self, source_cmd, stmts, hostname, port, database, username):
        for stmt in stmts:
            run_stmt = source_cmd + ' && psql -p %s -d %s -U %s -c \'%s\'' \
                    % (port, database, username, stmt)
            print run_stmt
            os.system(run_stmt)

    def run_sql(self, filename):
        os.system('psql -f %s'%filename)

    def addUserPGHBA(self, master_data_dir, kind = 'greenplum'):
        """Add user authentication entry in $MASTER_DATA_DIRECTORY/pg_hba.conf"""
        f = master_data_dir + '/pg_hba.conf'
        pg_hba_lines = [ lin.strip() for lin in open(f).readlines() ]
        fh = open(f, "a")
        for pghbastr in self.pghbastrs:
            if pghbastr.strip() in pg_hba_lines:
                continue
            else:
                fh.write("\n%s" % (pghbastr))

        fh.close()
        if kind.upper() == 'GREENPLUM':
            os.system("gpstop -u -d %s"%master_data_dir)
        elif kind.upper() == 'POSTGRES':
            os.system("pg_ctl reload -D %s -s"%master_data_dir)


    def remUserPGHBA(self, master_data_dir):
        """remove user authentication entry in $MASTER_DATA_DIRECTORY/pg_hba.conf
        
        not useful currently
        """
        fin = master_data_dir + '/pg_hba.conf'
        fot = master_data_dir + '/pg_hba.conf.backup'
        fhin = open(fin, "r")
        fhot = open(fot, "w")
        for line in fhin:
            if not line in self.pghbastrs:
                fhot.write(line)
        fhin.close()
        fhot.close()

        os.remove(fin)
        os.rename(fot, fin)

        os.system("gpstop -u")

def getMasterDir():
    tools = AnalyticsTools(AnalyticsTool)
    tools.parserTools()

    l = []
    for name, _ in tools.analyticsTools.items():
        l.append(tools.analyticsTools[name]["master_dir"])
    return l

def getIpv4Addr():
    host_name = socket.gethostname()
    host_info = socket.getaddrinfo(host_name, None)
    ipv4_list = ['127.0.0.1']
    ipv4_list.extend([ ip for ip in list(set([(ai[4][0]) 
                    for ai in host_info])) if ip.find(".") > 0])
    return ipv4_list


