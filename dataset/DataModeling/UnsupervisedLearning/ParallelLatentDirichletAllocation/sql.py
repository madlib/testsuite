import os, sys

file_list = os.listdir('.')

table = sys.argv[1]
sql = sys.argv[2]

outfile = open(sql, 'w')

dictfile = open(table + '.dict')
docfile = open(table + '.madlib')

outfile.write("DROP TABLE IF EXISTS %s_dict;\n" % table)
outfile.write("CREATE TABLE %s_dict(dict text[]) DISTRIBUTED RANDOMLY;\n" % table)
outfile.write("ALTER TABLE %s_dict OWNER TO madlibtester;\n" % table)
outfile.write("INSERT INTO %s_dict VALUES(ARRAY[%s]);\n" % (table, dictfile.readline()))

dictfile.close()

outfile.write("DROP TABLE IF EXISTS %s;\n" % table)
outfile.write("CREATE TABLE %s(id int4, contents int4[]);\n" % table)
outfile.write("ALTER TABLE %s OWNER TO madlibtester;\n" % table)
for article in docfile.readlines():
    if len(article.split(':')) == 2:
        outfile.write("INSERT INTO %s VALUES(%s, '{%s}');\n" % (table, article.split(':')[0], article.split(':')[1]))
docfile.close()

outfile.close()
