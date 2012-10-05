#!/usr/bin/env python
"""This file is to parse algorithmspec.xml.

There are multiply method for one algorithm.For each method, the template is the
invoking sql of madlib. The input parameters is specified in test case spec, 
and the output parameters are captured from the output of invoking sql. Both 
the input and output parameters willbe stored in result database.

<?xml version='1.0' encoding='UTF-8'?>
<algorithms>
    <algorithm>
         <name>kmeans</name>
         <method>        
            <name>canopy</name>
            <template>
                ...
            </template>
            <input_parameter>
                ....         
            </input_parameter>
        </method>
    </algorithms>
</algorithm> 
"""
from xml_parser import *
from file_path import *
from argparse import ArgumentParser
import sys
import db_out
from run_sql import parseConnectionStr, runSQL, Timer


class ExecutorSpec(Parser):
    def __init__(self, xml, db_schema = 'madlib'):
        Parser.__init__(self, xml)
        self.algorithms     = {}

        algorithms          =   Parser.getNodeTag(self, self.xmlDoc, "algorithms")
        algorithm_list      =   Parser.getNodeList(self, algorithms, "algorithm")
        self.algorithm_map  =   {}
        self.algorithms     =   [AlgorithmTemplate(algorithm, db_schema) 
                                 for algorithm in algorithm_list]
        for algorithm in self.algorithms:
            self.algorithm_map[algorithm.name] = algorithm

    def __str__(self):
        return '\n'.join([str(algorithm) for algorithm in self.algorithms])

    def getAlgorithm(self, algorithm):
        """
        algorithm: the name of algorithm
        algorithm class
        """
        return self.algorithm_map[algorithm]

    def writeCreateSQL(self, file):
        """Write the sql file to create the algorithm result table, which will 
        be stored after successfully invocation
    
        param file: file name to be wrote
        """

        f = open(file, "w")
        for algorithm, template in self.algorithm_map.items():
            [ f.write(method.generateCreateSQL(algorithm)) 
                for method in template.methods ]

class AlgorithmTemplate(Parser):
    """The tag of <algorithm> under the <algorithms> tag"""

    def __init__(self, node, db_schema):
        self.top_node = node

        self.name       =   Parser.getNodeVal(self, self.top_node,"name")
        method_nodes    =   Parser.getNodeList(self, self.top_node, "method")
        self.methods    =   [MethodTemplate(method_node, db_schema)
                                for method_node in method_nodes]

        self.method_map =   {}
        for method in self.methods:
            self.method_map[method.name] = method

    def __str__(self):
        return self.name+ ', ' + '\t'.join([str(method) for method in self.methods])

    def getMethod(self, method):
        return self.method_map[method]

class MethodTemplate(Parser):
    """The tag of <method> under the <algorithm> tag"""

    def __init__(self, node, db_schema):
        self.top_node = node
        self.db_schema = db_schema

        self.name           =   Parser.getNodeVal(self, self.top_node,"name")
        self.create         =   Parser.getNodeVal(self, self.top_node, "create")
        self.template       =   Parser.getNodeVal(self, self.top_node, "template")
        input_para_nodes    =   Parser.getNodeList(self, self.top_node, "input_parameter")
        self.input_paras    =   [InputParameter(input_para_node) 
                                    for input_para_node in input_para_nodes]
        self.output_paras   =   [InputParameter(output_para_node) for output_para_node 
                                    in Parser.getNodeList(self, self.top_node, "output_parameter")]


        self.para_map       =   {}
        for para in self.output_paras:
            self.para_map[para.name] = para
        # input parameter can overwrite output parameter
        for para in self.input_paras:
            self.para_map[para.name] = para

    def __str__(self):
        return self.name + ', ' + self.template+ ', ' + \
            '\n'.join([str(input_para) for input_para in self.input_paras])

    def getInputParams(self):
        return self.input_paras

    def getOutputParams(self):
        return self.output_paras

    def parseParaValues(self, name_value_map, disable_quote):
        """Return the name value map with quote if need
    
        name_value_map: the input map of name to value, but the name
            may be out of scope of this method that should be filtered
        disable_quote: if the quote of the value is disable
        the filtered value value map with quote if need
        """

        new_map = {}
        for name, value in name_value_map.items():
            # name_value_map may include other environments
            if value == 'NINFINITY': value = '-INFINITY'

            if self.para_map.has_key(name):
                para = self.para_map[name]
                new_map[name] = para.getValue(value, disable_quote)
            else:
                new_map[name] = value
        return new_map

    def generateCreateSQL(self, algorithm):
        """return the sql statement to create the algorithm result table,
        which will be stored after successfully invocation


    
        param algorithm: algorithm name
        return sql statement
        """
        if self.create :
            #drop_table = "DROP TABLE IF EXISTS " + self.db_schema + \
            #    "." +  algorithm + "_" + self.name + ';'
            header = "CREATE TABLE " + self.db_schema + "." +  algorithm + \
                "_" + self.name +"(testitemname text, runid int, "
            paras = []
            paras.extend(self.input_paras)
            paras.extend(self.output_paras)
            #stmt = drop_table + header + ','.join(\
            stmt = header + ','.join(\
                   [ para.name + ' ' + para.type + ' default null ' for para in paras]) + ');\n\n'
            return stmt
        else :
            return ""

class InputParameter(Parser):
    """The tag of <InputParameter> under the <method> tag."""

    def __init__(self, node):
        self.top_node = node

        # input parameter name
        self.name = Parser.getNodeVal(self, self.top_node,"name").lower()
        # input parameter value
        self.type = Parser.getNodeVal(self, self.top_node,"type").lower()
        # should we quote the parameter name?
        quote = Parser.getNodeVal(self, self.top_node,"quote")
        # default is True
        if not quote:
            self.quote = True
        elif quote.lower in ('t', 'true'):
            self.quote = True
        else:
            self.quote = False

        try:
            self.default = Parser.getNodeVal(self, self.top_node,"default")
        except Exception:
            self.default = ''

    def __str__(self):
        return self.name+ ', ' + self.type+ ', ' + self.default

    def getName(self):
        return self.name

    def getDefaultValue(self):
        return self.default


    """
    @brief return the value of this parameter.
    @param value: text value
    @para disable_quote: is quote disable?
    @return value
    """
    def getValue(self, value, disable_quote):
        """Return the value of this parameter.
    
        params:
            value: text value
            disable_quote: is quote disable?
        return value
        """

        #NULL value for any type
        if value == 'NULL':
            return value    
        #is the invoke need quote and dose the value support quote itself?
        if disable_quote and not self.quote:    
            return value    
        if self.type == 'text' and value == 'EMPTY':
            return "''" #empty string
        else:
            #return "'%s'::%s" % (value, self.type)
	    value = value.replace("ARRAY[","{")
	    value = value.replace("]","}")
            return "$_valString$%s$_valString$::%s" % (value, self.type)

class Executor:
    """The Template Executor to execute MADLIB invocation.

    It can also execute sql or just print sql statement.
    """

    def __init__(self, argv, logger):
        self.logger = db_out.Logger(argv)
        self.argv = argv

    def parseArgument(self):
        """Parse method specified options.There are two kinds of options.
    
        1) general options such as madlib_schema, algorithm, method, 
            target_base_name(test item name) and so on.
        2) specific options that come from algorithm, method above
        """

        #1) general options
        parser = ArgumentParser(description="MADLib Executor")
        parser.add_argument("--p"               , type=str, required = False, 
                            default = 'greenplum')
        parser.add_argument("--madlib_schema"   , type=str, required = False
                            , default = 'madlib')
        parser.add_argument("--algorithm"       , type=str, required = True)
        parser.add_argument("--method"          , type=str, required = True)
        parser.add_argument("--target_base_name", type=str, required = True)
        parser.add_argument("--run_id"          , type=str, required = False
                            , default = '0')
        parser.add_argument("--spec_file"       , type=str, required = False
                            , default = '../xml/config/' + Path.algorithmsSpecXml)
        parser.add_argument("--conn", "-C",
            metavar = "CONNSTR",
            default = '',
            dest = 'connection_string')

        parser.parse_known_args(args = self.argv, namespace = self)
        (self.logusername, self.logpassword, self.loghostname, self.logport,
            self.logdatabase, _) = parseConnectionStr(self.connection_string)

        #come out the executor specification
        self.spec = ExecutorSpec(self.spec_file, self.madlib_schema)

        #2) specific options
        parser = ArgumentParser(description="MADLib Executor")
        method = self.spec.getAlgorithm(self.algorithm).getMethod(self.method)
        paras = method.getInputParams()
        for para in paras:
            if para.getDefaultValue():
                parser.add_argument("--"+para.getName(), type=str, required = False
                    , default = para.getDefaultValue())
            else:
                parser.add_argument("--"+para.getName(), type=str, required = True)

        try:
            argument, _ = parser.parse_known_args(args = self.argv, namespace = self)
            self.argument = argument
        except Exception as e:
            print 'Error in ArgumentParser', str(e)

    def generateSQL(self):
        """return the madlib invoking sql statement."""

        method = self.spec.getAlgorithm(self.algorithm).getMethod(self.method)
        para_mapping = method.parseParaValues(self.argument.__dict__, True)
        self.sql = method.template.format(**para_mapping)
        return self.sql

    def run(self):
        """Run the madlib invoking sql statement, and store the result to logger database."""

        is_successful = False
        sql = self.sql
        #print executor command and invocation sql statement
        print '----------------------------------------------------'
        print ' '.join(self.argv)
        print self.sql
        timer = Timer()
        #the elapsed time is in second, but we will convert it to ms in database
        with timer:
            try:
                result = runSQL(
                    sql
                    , self.logusername, self.logpassword, self.loghostname
                    , self.logport, self.logdatabase
                    , ['--expanded']
                    )
                is_successful = True
            except Exception as e:
                result = str(e)
                print str(e)
        try:
            #strip empty line, and use only the tail of 1k char
            result = '\n'.join( [ l for l in result.splitlines() if l.strip() != '' ] )
            result = result[-1000:]
            self.logger.log_test_item(timer.elapsed, is_successful, "table", \
                                      result, sql, "false")
            if is_successful:
                self.__store_db_result(result)
        except Exception as e:
            print 'ERROR', str(e)

    def __store_db_result(self, result):
        """store the result to logger database"""

        log_schema = self.logger.logger_schema
        method = self.spec.getAlgorithm(self.algorithm).getMethod(self.method)

        if method.create:
            paras = []
            paras.extend(method.input_paras)
            paras.extend(method.output_paras)
            insert_sql = 'INSERT INTO {logger_schema}.' +  self.algorithm + "_" + self.method +\
                            "(testitemname, runid, " + ','.join([para.name for para in paras]) + \
                            ") VALUES ( '{testitemname}', {run_id},  "+  \
                            ','.join(["{" + para.name + "}" for para in paras]) + \
                            " )"
            output_paras = {}
            if method.output_paras:
                #need to capture output parameters
                for line in result.splitlines():
                    tokens = line.split('|')
                    if len(tokens) > 1:
                        value = tokens[1].strip()
                        if value == '':
                            value = 'NULL'
                        output_paras[tokens[0].strip()] = value[-4000:]
                output_paras = method.parseParaValues(output_paras, False)
            #merge input parameters
            output_paras.update(**method.parseParaValues(self.argument.__dict__, False))
            insert_sql = insert_sql.format(
                    logger_schema = log_schema
                    , testitemname = self.target_base_name
                    , **output_paras)

            print insert_sql
            self.logger.runSQL(insert_sql)

def main():
    """Parse the argument options, generate sql, run and store result."""

    exe = Executor(sys.argv, None)
    exe.parseArgument()
    exe.generateSQL()
    exe.run()

if __name__ == '__main__':
    main()
