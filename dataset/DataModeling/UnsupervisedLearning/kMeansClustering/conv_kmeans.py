#!/usr/bin/env python
import sys, os

data_dir = './'

def main():
    from optparse import OptionParser
    import glob
    use = '''usage: ./conv_linregr.py --dataset name   --output  outfile
            --skip_missing_value  
            --add_constant_col
    '''
    parser = OptionParser(usage=use)
    parser.add_option("-t", "--table", action="store", dest="table", type="string")
    parser.add_option("-o", "--output", action="store", dest="output", type="string", default = None)
    parser.add_option("-d", "--data", action="store", dest="data", type="string")
    parser.add_option("-D", "--desc", action="store", dest="desc", type="string")
    parser.add_option("-l", "--listcolumn", action="store_true", dest="listcolumn", default = False)
    parser.add_option("-s", "--skip_missing_value", action="store_true", default = False)
    parser.add_option("-c", "--add_constant_col", action="store_true", default = False)
    (options, args) = parser.parse_args()
    
    desc_file = open(options.desc)
    data_file = open(options.data)
    if not options.output:
        options.output = options.dataset+'.out'
    out_file = open(options.output, 'w')
    
    import re
    col_desc = [ [ l for l in re.split('\W+', line.strip().lower()) if l != '' ] for line in desc_file.readlines()]
    num_col_list = []
    classes = {}
    for i  in range(len(col_desc)):
        if col_desc[i][1] in ('int', 'float8'):
            num_col_list.append(i)
    
    num_col_desc = [ l for l in col_desc if l[1] in ('int', 'float8')]
    rows = 0
    out_file.write("SET client_min_messages TO WARNING;DROP TABLE IF EXISTS %s;\n" % options.table)
    out_file.write("CREATE TABLE %s (pid bigint, position float8[]);\n" % (options.table))
    out_file.write("COPY %s FROM STDIN NULL '?' DELIMITER ' ';\n" % options.table)
    if 'red' in options.table or 'census' in options.table:
        data_file.readline()
    while 1:
        data = data_file.readline().strip()
        if not data:
            break;
        in_cols = [ l for l in re.split('[\s,;]+', data.strip())  if l != '' ]
        out_cols = []
        
        has_missing_value = False
        for i in  num_col_list:
            if in_cols[i] == '?':
                if options.skip_missing_value:
                    has_missing_value = True
                out_cols.append('NaN')
            else:
                out_cols.append(in_cols[i])
        if options.skip_missing_value and has_missing_value:
            continue
        out_file.write('%d {%s}\t\n' % (rows+1, ','.join(out_cols)))
                
        rows = rows + 1

    out_file.write("\\.\n")
    out_file.write("ALTER TABLE %s OWNER TO madlibtester;" % options.table)
    out_file.close()
if __name__ == '__main__':
    main()
