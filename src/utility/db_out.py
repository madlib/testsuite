"""DB out logger to store madlib invocation from the testing database."""
import run_sql

class Logger:
    def __init__(self, args):
        self.args = args

        from argparse import (ArgumentParser, RawTextHelpFormatter)
        parser = ArgumentParser(description = 'MADlib performance testing result DB LOGGER',
                                argument_default = False,
                                formatter_class = RawTextHelpFormatter,
                                epilog='FIXME')
        parser.add_argument('--logger_conn', 
                            metavar = 'CONNSTR',
                            default = '',
                            dest = 'connection_string')
        parser.add_argument('--iteration_id',       type = int, required = True)
        parser.add_argument('--analyticstool',      type = str, required = False, default = '')
        parser.add_argument('--target_base_name',   type = str, required = True)
        parser.add_argument('--run_id'          ,   type = str, required = False, default = '0')
        
        (_, _) = parser.parse_known_args(args = self.args, namespace = self)
        (self.logusername, self.logpassword, self.loghostname, self.logport,
            self.logdatabase, logger_schema) = run_sql.parseConnectionStr(self.connection_string)
        if logger_schema:
            self.logger_schema = logger_schema

    def log_test_item(self, elapsed, is_successful, result_type, result_info, \
                      command, is_verification):
        """Insert general result to the result database.

        params:
            elapsed : elapsed time of the MADlib invocation, is measured in ms, 
            but in database, the elapsed time is measured in second.
            is_successful : is the MADlib invocation successful?
            result_type : choice of 'table' or 'output'
            result_info : the stdout of the MADlib invocation
            command :  the command of the MADlib invocation
            is_verification :  is the command a verification method?
        """

        command = command.replace("'", '"')
        result_info = result_info.replace("'", '"')
        sql = """insert into %s.testitemresult
                values( '%s', %s, %s, '%s', 
                '%s', %s, %s::bool, '%s', '%s', %s::bool);
              """ % (self.logger_schema, self.target_base_name, self.run_id, \
                    self.iteration_id, result_type, self.analyticstool, elapsed*1000, 
                    is_successful, str(result_info), command, is_verification)
        print sql
        run_sql.runSQL(sql, self.logusername, self.logpassword, self.loghostname, 
               self.logport, self.logdatabase)

    def runSQL(self, sql):
        """Run sql statement in this logger database
    
        params:
            sql statement to run
        """
        run_sql.runSQL(sql, self.logusername, self.logpassword, self.loghostname, \
               self.logport, self.logdatabase)
