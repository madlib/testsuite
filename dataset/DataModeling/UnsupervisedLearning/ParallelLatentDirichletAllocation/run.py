import os, sys
import utility
import re
import time


def prepare_reuters21578(r):
    result = []
    total_topics = {}
    total_texts = {}
    #outfile = open(r.split('/')[-1]+'.topic','w')
    file_path_list, file_name_list = utility.get_file_list(r)
    for file_path in file_path_list:
        if file_path.endswith("sgm"):
            parser = utility.MyParser()
            parser.parse(open(file_path).read())
            total_topics.update(parser.get_topics())
            total_texts.update(parser.get_texts())
    #outfile.close()
    return total_topics, total_texts

def filter_for_email(file_path):
    infile = open(file_path, 'r')
    result = []
    line = infile.readline()
    while line:
        if line.startswith('Lines'):
            try:
                row = int(line.split(':')[1])
            except:
                print "cannot parse %s to int." % line.split(':')[1]
                break
            for i in range(0,row + 1):
                result.append(infile.readline())
            break
        line = infile.readline()
    infile.close()
    return result

def filter_for_unfilter(file_path):
    infile = open(file_path, 'r')
    lines = infile.readlines()
    infile.close()
    return lines

#def topic_by_foldername(r):
#    file_path_list, file_name_list = utility.get_file_list(r)
#    outfile = open(r + ".topic", 'w')
#    for file_path in file_path_list:
#        topic = file_path.split('/')[1]
#        name  = file_path.split('/')[2]
#        outfile.write(name + ' ' + topic + '\n')
#    outfile.close()


def dict_by_dict(r, dict, table):
    total_words = []
    for key, value in dict.iteritems():
        total_words += utility.get_file_words(value.split('\n'))
    dict = utility.unique(total_words)
    utility.write_dict_file(dict, table + '.dict')
    return dict


def dict_by_text(r, table, parse_method = None):
    file_path_list, file_name_list = utility.get_file_list(r)
    total_words = []
    for file_path in file_path_list:
        lines = parse_method(file_path)
        total_words += utility.get_file_words(lines)
    dict = utility.unique(total_words)
    utility.write_dict_file(dict, table + '.dict')
    return dict

def dict_by_file(r, table, parse_method = None):
    file_path_list, file_name_list = utility.get_file_list(r)
    total_words = []
    for file_path in file_path_list:
        if file_path.startswith('all'):
            total_words += open(file_path, 'r').readlines()
    dict = utility.unique(total_words)
    utility.write_dict_file(dict, r.split('/')[-1] + '.dict')
    return dict

def parse_sgml(r, table):
    start_time = time.time()
    total_topics, total_texts = prepare_reuters21578(r)
    dict = dict_by_dict(r, total_texts, table)

    madlib = open(table + '.madlib', 'w')

    j = 1
    for key, value in total_texts.iteritems():
        #print key
        lines = value.split('\n')
        words = utility.get_file_words(lines)
   
        if len(words) == 0: continue
        #for madlib

        line = utility.get_madlib_line(words, dict)
        madlib.write(('%s : %s') % (str(j), line))
        j = j + 1
    madlib.close()
    elapsed_time = time.time() - start_time
    print r, 'parsing time:', elapsed_time

def parse_nsf(r, table):
    start_time = time.time()
    dict_in_name = r + '/words.txt'
    infile = open(dict_in_name, 'r')
    total_words = [w.strip() for w in infile.readlines()]
    #for line in infile.readlines():
    #    total_words.append(line.split()[1].strip())
    dict_out_name = table + '.dict'
    outfile = open(dict_out_name, 'w')
    outfile.write("'")
    outfile.write("','".join(total_words))
    outfile.write("'")
    infile.close()
    outfile.close()

    file_path_list, file_name_list = utility.get_file_list(r)

    madlib = open(table + '.madlib', 'w')
    madlib_out = {}
    google_out = {}
    R_out      = {}
    for file_path in file_path_list:
        if file_path.endswith("docwords.txt"):
            infile = open(file_path)
            for line in infile.readlines():
                parts = line.split()
                if int(parts[1]) >= len(total_words): continue
                if parts[0] in madlib_out:
                    for i in range(0, int(parts[2])):
                        madlib_out[parts[0]] += ',' + parts[1]
                else:
                    madlib_out[parts[0]] = parts[1]
                if parts[0] in google_out:
                    google_out[parts[0]] += total_words[int(parts[1]) - 1] + ' ' + parts[2] + ' '
                else:
                    google_out[parts[0]] = total_words[int(parts[1]) - 1] + ' ' + parts[2] + ' '
                if parts[0] in R_out:
                    R_out[parts[0]] += parts[1] + ':' + parts[2] + ' '
                else:
                    R_out[parts[0]] = parts[1] + ':' + parts[2] + ' '

    j = 1
    for key, value in madlib_out.iteritems():
        madlib.write(str(j) + ":" + value + '\n')
        j = j + 1

#    for key, value in google_out.iteritems():
#        google.write(value + '\n')

#    for key, value in R_out.iteritems():
#        R.write( str(len(value.split(':'))+1) + ' ' + value + '\n')
    
    madlib.close()

    elapsed_time = time.time() - start_time
    print r, 'parsing time:', elapsed_time

def parse_normal(r, parse_method, dict_method, table):
    dataset = table
    start_time = time.time()
    #topic_method(r)
    file_path_list, file_name_list = utility.get_file_list(r)
    dict  = dict_method(r, table, parse_method)

    madlib = open(dataset + '.madlib', 'w')

    file_count = len(file_path_list)

    for i in range(0, file_count):
        lines = parse_method(file_path_list[i])
        words = utility.get_file_words(lines)
        if len(words) == 0: continue
        #for madlib
        
        line = utility.get_madlib_line(words, dict)
        madlib.write(('%s : %s') % (str(i + 1), line))

    madlib.close()
    elapsed_time = time.time() - start_time
    print r, 'parsing time:', elapsed_time

if __name__ == "__main__":
    data = sys.argv[1]
    table = sys.argv[2]
    if table == 'madlibtestdata.plda_20_newsgroups':
        parse_normal(data, filter_for_email, dict_by_text, table)
    if table == 'madlibtestdata.plda_mini_20_newsgroups':
        parse_normal(data, filter_for_email, dict_by_text, table)
    if table == 'madlibtestdata.plda_reutersTranscribedSubset':
        parse_normal(data, filter_for_unfilter, dict_by_text, table) 
    if table == 'madlibtestdata.plda_reuters21578':
        parse_sgml(data, table)
    if table == 'madlibtestdata.plda_nsf_abs':
        parse_nsf(data, table)

