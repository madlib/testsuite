import sys, os

data=sys.argv[1]
label=sys.argv[2]
desc=sys.argv[3]
sql=sys.argv[4]
table=sys.argv[5]


data_lines = open(data).readlines()
label_lines = open(label).readlines()

out = open('./madelon_data_label', 'w')
for i in range(0, 2000):
    out.write('%s %s' % (data_lines[i].strip(), label_lines[i]))
out.close()

import subprocess
subprocess.check_call('./convertor.sh %s %s %s %s' % ('madelon_data_label', sql, table, desc), shell = True)
os.remove('madelon_data_label')

    

