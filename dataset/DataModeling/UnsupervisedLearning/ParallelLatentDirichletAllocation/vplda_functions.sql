CREATE LANGUAGE plpythonu;

DROP FUNCTION madlibtestdata.vplda_acenter (table_array TEXT[])  ;
DROP FUNCTION madlibtestdata.vplda_stability( num_runs int, dict_table text, model_table text, num_topics int, alpha float, eta float,
                                                   train_corpus_table text, train_output_data_table text, train_num_iter int,
                                                   test_corpus_table text, test_output_table text )  ;
DROP FUNCTION madlibtestdata.vplda_similarity( num_runs int, dict_table text, model_table text, num_topics int, alpha float, eta float,
                                                    train_corpus_table text, train_output_data_table text, train_num_iter int,
                                                    test_corpus_table text, test_output_table text,
                                                    glda_word_topic_dist_table_names text, glda_doc_topic_dist_table_names text,
                                                    gplda_word_topic_dist_table_names text, gplda_doc_topic_dist_table_names text,
                                                    rlda_word_topic_dist_table_names text, rlda_doc_topic_dist_table_names text )  ;
DROP FUNCTION madlibtestdata.vplda_avgkl (table_array TEXT[])  ;
DROP FUNCTION madlibtestdata.vplda_kl( dist_table_name_1 text, dist_table_name_2 text )  ;
DROP FUNCTION madlibtestdata.vplda_div( num1 float8, num2 float8, div_type text )  ;
DROP TYPE madlibtestdata.vplda_dist  ;
DROP TYPE madlibtestdata.vplda_similarity_result;
                                                                                                             


CREATE OR REPLACE FUNCTION madlibtestdata.vplda_div( num1 float8, num2 float8, div_type text )
RETURNS float8
LANGUAGE plpythonu
VOLATILE
RETURNS NULL ON NULL INPUT
AS $$
    import math
    dtype = div_type.lower()
    if ( not dtype in ['symmetric', 'asymmetric'] ):
        plpy.error( "Error: invalid divergence type " + div_type + " is specified" )

    dvalu = 0.0
    log_num1 = 0.0
    log_num2 = 0.0
    if ( math.fabs( num1 ) >= 1.0E-9 ):
        log_num1 = math.log( num1, 2 )
    else: return 0 
    if ( math.fabs( num2 ) >= 1.0E-9 ):
        log_num2 = math.log( num2, 2)
    else: return 0
    dvalu += num1 * ( log_num1 - log_num2)

    if dtype == 'symmetric':
        dvalu += num2 * (log_num2 - log_num1)
        dvalu /= 2.0
    return dvalu
$$;

CREATE OR REPLACE FUNCTION madlibtestdata.vplda_kl( dist_table_name_1 text, dist_table_name_2 text )
RETURNS float8[]
LANGUAGE plpythonu
VOLATILE
RETURNS NULL ON NULL INPUT
AS $$
    import math

    # check the validity of parameters
    row_size1_t = plpy.execute( "SELECT count(*) rsize FROM %s;" % dist_table_name_1 )
    col_size1_t = plpy.execute( "SELECT DISTINCT array_upper( dist, 1 ) csize FROM %s;" % dist_table_name_1 )
    row_size2_t = plpy.execute( "SELECT count(*) rsize FROM %s;" % dist_table_name_2 )
    col_size2_t = plpy.execute( "SELECT DISTINCT array_upper( dist, 1 ) csize FROM %s;" % dist_table_name_2 )

    if ( row_size1_t.nrows() <> 1 or col_size1_t.nrows() <> 1 ):
        plpy.error( "Error: distribution table %s is not well formed" % dist_table_name_1 )

    if ( row_size2_t.nrows() <> 1 or col_size2_t.nrows() <> 1 ):
        plpy.error( "Error: distribution table %s is not well formed" % dist_table_name_2 )

    row_size1 = row_size1_t[0]['rsize']
    col_size1 = col_size1_t[0]['csize']
    row_size2 = row_size2_t[0]['rsize']
    col_size2 = col_size2_t[0]['csize']

    if ( row_size1 <= 0 or col_size1 <= 0 ):
        plpy.error( "Error: distribution table %s is not well formed" % dist_table_name_1 )

    if ( row_size2 <= 0 or col_size2 <= 0 ):
        plpy.error( "Error: distribution table %s is not well formed" % dist_table_name_2 )

    if ( row_size1 <> row_size2 or col_size1 <> col_size2 ):
        plpy.error("row_size1 %d, row_size2 %d, col_size1 %d, col_size2 %d" %(row_size1,row_size2,col_size1,col_size2))

        plpy.error( "Error: distribution tables %s and %s are not in the same form" % (dist_table_name_1, dist_table_name_2) )

    row_size = row_size1
    col_size = col_size1
    # normalize the two distributions
    dist1_t = plpy.execute( "SELECT dist FROM %s ORDER BY id;" % dist_table_name_1 )
    dist2_t = plpy.execute( "SELECT dist FROM %s ORDER BY id;" % dist_table_name_2 )
 
    dist1 = []
    dist2 = []
    for i in range( row_size ):
        dist1.append( dist1_t[i]['dist'] )
        dist2.append( dist2_t[i]['dist'] )

    for i in range( row_size ):
        sum1 = 0.0
        sum2 = 0.0
        for j in range( col_size ):
            sum1 += dist1[i][j]
            sum2 += dist2[i][j]

        if ( abs( sum1 ) < 1.0E-9 ):
            plpy.error( "Error: distribution table %s has flaw data" % dist_table_name_1 )

        if ( abs( sum2 ) < 1.0E-9 ):
            plpy.error( "Error: distribution table %s has flaw data" % dist_table_name_2 )

        for j in range( col_size ):
            dist1[i][j] /= sum1
            dist2[i][j] /= sum2

        if sum(dist1[i]) - 1 > 1.0E-9:
            plpy.info("Normanized ERROR!")

    # compute the similarity matrix of the two distributions using symmetric Kullback Liebler distance
    sim_matrix = []
    for i in range( col_size ):
        sim_matrix.append( [] )
    for j in range( col_size ):
        for l in range( col_size ) :
            dvalu = 0.0
            for k in range( row_size ):
                # calculate divergence of two float numbers
                log_num1 = 0.0
                log_num2 = 0.0

                if ( math.fabs( dist1[k][l] ) >= 1.0E-9 and math.fabs( dist2[k][j] ) >= 1.0E-9 ):
                    log_num1 = math.log( dist1[k][l], 2 )
                    log_num2 = math.log( dist2[k][j], 2 )
                    dvalu += dist1[k][l] * ( log_num1 - log_num2 )
                    dvalu += dist2[k][j] * ( log_num2 - log_num1 )
                    dvalu /= 2.0

            sim_matrix[j].append( dvalu )

    # sort the topics of the two distributions using greedy algorithm
    for i in range( col_size ):
        min_val = sim_matrix[i][i]
        min_row = i
        min_col = i
        for j in range( i, col_size ):
            for k in range( i, col_size ):
               if ( sim_matrix[j][k] < min_val ):
                   min_val = sim_matrix[j][k]
                   min_row = j
                   min_col = k

        for j in range( 0, col_size ):
            tmp_val = sim_matrix[i][j]
            sim_matrix[i][j] = sim_matrix[min_row][j]
            sim_matrix[min_row][j] = tmp_val

        for j in range( 0, col_size ):
            tmp_val = sim_matrix[j][i]
            sim_matrix[j][i] = sim_matrix[j][min_col]
            sim_matrix[j][min_col] = tmp_val

    # compute the min/max/avg weights on the diagonal of similarity matrix
    sim_min = sim_matrix[0][0]
    sim_max = sim_matrix[col_size-1][col_size-1]
    sim_avg = 0.0
    for i in range( col_size ):
        sim_avg += sim_matrix[i][i]
    sim_avg /= col_size
    return [sim_min, sim_max, sim_avg]

$$;


CREATE OR REPLACE FUNCTION madlibtestdata.vplda_avgkl (table_array TEXT[])
    RETURNS float8[] 
    VOLATILE
    RETURNS NULL ON NULL INPUT
AS $$

result_set = []

length = len(table_array)
for idx in range(0, length -1):
    for idy in range(idx + 1, length):
        sql = "SELECT madlibtestdata.vplda_kl('%s', '%s');" % (table_array[idx], table_array[idy])
        rset_t = plpy.execute(sql)
        rset = rset_t[0]['vplda_kl']
        result_set.append(rset)
        # result_set.append(plpy.execute(sql)[0]['vplda_kl'])

array = [kl[0] for kl in result_set]
avg_max = sum(array) / len(result_set)
avg_min = sum([kl[1] for kl in result_set]) / len(result_set)
avg_avg = sum([kl[2] for kl in result_set]) / len(result_set)

result = [avg_max, avg_min, avg_avg]

plpy.info(result)
return result 

$$ LANGUAGE plpythonu;

CREATE TYPE madlibtestdata.vplda_dist AS (
    id          INTEGER,
    dist        FLOAT[]
);

CREATE OR REPLACE FUNCTION madlibtestdata.vplda_acenter (table_array TEXT[])
    RETURNS SETOF madlibtestdata.vplda_dist
    VOLATILE
    RETURNS NULL ON NULL INPUT
AS $$
result_table = []
temp = []

for table_name in table_array:
    sql = "SELECT dist FROM %s ORDER BY id;" % table_name
    temp.append([r['dist'] for r in plpy.execute(sql)])

table_width = len(temp[0][0])
table_height  = len(temp[0])

for y in range(0, table_height):
    new_row = []
    for x in range(0, table_width):

        temp_val_list = [float(temp_table[y][x]) for temp_table in temp]
        avg_val = sum(temp_val_list) / len(temp_val_list)

        new_row.append(avg_val)

    if sum(new_row) == 0:
        plpy.info(new_row)

    result_table.append([y+1, new_row])

return result_table
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION madlibtestdata.vplda_stability( num_runs int, dict_table text, model_table text, num_topics int, alpha float, eta float,
                                                   train_corpus_table text, train_output_data_table text, train_num_iter int,
                                                   test_corpus_table text, test_output_table text )
RETURNS float8[]
LANGUAGE plpythonu
VOLATILE
RETURNS NULL ON NULL INPUT
AS $$
    # check the validity of parameters
    if ( num_runs <= 1 ):
        plpy.error( "Error: to evaluate the stability of plda, the number of plda runs should be greater than 1" )

    dict_size_t = plpy.execute( "SELECT array_upper( dict, 1 ) dict_size FROM %s;" % dict_table )
    if ( dict_size_t.nrows() <> 1 ):
        plpy.error( "Error: dictionary table %s is not well formed" % dict_table )
    dict_size = dict_size_t[0]['dict_size']

    # runs plda_run and plda_label_test_documents for multiple times
    mplda_word_topic_dist_table_names = []
    mplda_doc_topic_dist_table_names = []
    for i in range( 1, num_runs+1 ):
        # clean up before each run of plda_run and plda_label_test_documents

        plpy.info("============================> run id : %d" % i)

        plpy.execute( "DROP TABLE IF EXISTS %s;" % model_table )
        plpy.execute( "DROP TABLE IF EXISTS %s;" % train_output_data_table )
        plpy.execute( "DROP TABLE IF EXISTS %s;" % test_output_table )
        plpy.execute( "DROP TABLE IF EXISTS madlibtestdata.mplda_word_topic_dist_%s;" % str(i) )
        plpy.execute( "DROP TABLE IF EXISTS madlibtestdata.mplda_doc_topic_dist_%s;" % str(i) )

        plpy.execute( "SELECT madlib.plda_run(\'%s\', \'%s\', \'%s', \'%s\', %d, %d, %f, %f);" %
                                             (train_corpus_table, dict_table, model_table, train_output_data_table, train_num_iter, num_topics, alpha, eta) )
        plpy.execute( "SELECT madlib.plda_label_test_documents(\'%s\', \'%s\', \'%s\', \'%s\', %d, %f, %f);" %
                                                              (test_corpus_table, test_output_table, model_table, dict_table, num_topics, alpha, eta) )

        plpy.execute( "CREATE TABLE madlibtestdata.mplda_word_topic_dist_%s AS SELECT ss.id, madlib.plda_word_topic_distrn(gcounts, %d, ss.id) dist FROM %s, (SELECT generate_series(1, %d) id) AS ss;" %
                                                                (str(i), num_topics, model_table, dict_size) )

        #plpy.execute( "ALTER TABLE madlibtestdata.mplda_word_topic_dist_%d OWNER TO madlibtester;" %i)

        #plpy.info("Check whether table is created.")
        #plpy.info(plpy.execute("SELECT * FROM madlibtestdata.mplda_word_topic_dist_%d;" % i)[0])

        plpy.execute( "CREATE TABLE madlibtestdata.mplda_doc_topic_dist_%s AS SELECT id, (topics).topic_d dist FROM %s;" %(str(i), test_output_table) )
        mplda_word_topic_dist_table_names.append( "madlibtestdata.mplda_word_topic_dist_%s" % str(i) )
        mplda_doc_topic_dist_table_names.append( "madlibtestdata.mplda_doc_topic_dist_%s" % str(i) )

    # compute stability of the word-topic and doc-topic distributions of the multiple plda_run and plda_label_test_documents runs respectively
    # stbs = [word_topic_stb_min, word_topic_stb_max, word_topic_stb_avg, doc_topic_stb_min, doc_topic_stb_max, doc_topic_stb_avg]
    stbs = []
    plpy.info("=================================> Finished running. Start calculate avg KL.")
    stbs_t = plpy.execute( "SELECT madlibtestdata.vplda_avgkl(ARRAY ['%s']);" % "','".join(mplda_word_topic_dist_table_names))
    plpy.info(stbs_t[0]['vplda_avgkl'])
    stbs += stbs_t[0]['vplda_avgkl']
      
    stbs_t = plpy.execute( "SELECT madlibtestdata.vplda_avgkl(ARRAY ['%s']);" % "','".join(mplda_doc_topic_dist_table_names ))
    plpy.info(stbs_t[0]['vplda_avgkl'])
    stbs += stbs_t[0]['vplda_avgkl'] 

    # clean up after mutiple runs of plda_run and plda_label_test_documents
    plpy.execute( "DROP TABLE IF EXISTS %s;" % model_table )
    plpy.execute( "DROP TABLE IF EXISTS %s;" % train_output_data_table )
    plpy.execute( "DROP TABLE IF EXISTS %s;" % test_output_table )
    # for i in range( 1, num_runs+1 ):
        # plpy.execute( "DROP TABLE IF EXISTS madlibtestdata.mplda_word_topic_dist_%s;" % str(i) )
        # plpy.execute( "DROP TABLE IF EXISTS madlibtestdata.mplda_doc_topic_dist_%s;" % str(i) )

    return stbs
$$;

CREATE TYPE madlibtestdata.vplda_similarity_result AS (
    implementations          TEXT,
    similarities  FLOAT[]
);


CREATE OR REPLACE FUNCTION madlibtestdata.vplda_similarity( num_runs int, dict_table text, model_table text, num_topics int, alpha float, eta float,
                                                    train_corpus_table text, train_output_data_table text, train_num_iter int,
                                                    test_corpus_table text, test_output_table text,
                                                    glda_word_topic_dist_table_name text, glda_doc_topic_dist_table_name text,
                                                    gplda_word_topic_dist_table_name text, gplda_doc_topic_dist_table_name text,
                                                    rlda_word_topic_dist_table_name text, rlda_doc_topic_dist_table_name text )
RETURNS SETOF madlibtestdata.vplda_similarity_result
LANGUAGE plpythonu
VOLATILE
AS $$

    # check the validity of parameters
    if ( num_runs <= 1 ):
        plpy.error( "Error: to evaluate the stability of plda, the number of plda runs should be greater than 1" )

    dict_size_t = plpy.execute( "SELECT array_upper( dict, 1 ) dict_size FROM %s;" % dict_table )
    if ( dict_size_t.nrows() <> 1 ):
        plpy.error( "Error: dictionary table %s is not well formed" % dict_table )
    dict_size = dict_size_t[0]['dict_size']

    # runs plda_run and plda_label_test_documents for multiple times
    mplda_word_topic_dist_table_names = []
    mplda_doc_topic_dist_table_names = []
    for i in range( 1, num_runs+1 ):
        # clean up before each run of plda_run and plda_label_test_documents

        plpy.info("============================> run id : %d" % i)

        plpy.execute( "DROP TABLE IF EXISTS %s;" % model_table )
        plpy.execute( "DROP TABLE IF EXISTS %s;" % train_output_data_table )
        plpy.execute( "DROP TABLE IF EXISTS %s;" % test_output_table )
        plpy.execute( "DROP TABLE IF EXISTS madlibtestdata.mplda_word_topic_dist_%s;" % str(i) )
        plpy.execute( "DROP TABLE IF EXISTS madlibtestdata.mplda_doc_topic_dist_%s;" % str(i) )

        plpy.execute( "SELECT madlib.plda_run(\'%s\', \'%s\', \'%s', \'%s\', %d, %d, %f, %f);" %
                                             (train_corpus_table, dict_table, model_table, train_output_data_table, train_num_iter, num_topics, alpha, eta) )
        plpy.execute( "SELECT madlib.plda_label_test_documents(\'%s\', \'%s\', \'%s\', \'%s\', %d, %f, %f);" %
                                                              (test_corpus_table, test_output_table, model_table, dict_table, num_topics, alpha, eta) )


        plpy.execute( "CREATE TABLE madlibtestdata.mplda_word_topic_dist_%s AS SELECT ss.id, madlib.plda_word_topic_distrn(gcounts, %d, ss.id) dist FROM %s, (SELECT generate_series(1, %d) id) AS ss;" % (str(i), num_topics, model_table, dict_size) )
        plpy.execute( "CREATE TABLE madlibtestdata.mplda_doc_topic_dist_%s AS SELECT id, (topics).topic_d dist FROM %s;" %(str(i), train_output_data_table) )
        mplda_word_topic_dist_table_names.append( "madlibtestdata.mplda_word_topic_dist_%s" % str(i) )
        mplda_doc_topic_dist_table_names.append( "madlibtestdata.mplda_doc_topic_dist_%s" % str(i) )

        wt = plpy.execute("SELECT * FROM madlibtestdata.mplda_word_topic_dist_%d" % i)
        dt = plpy.execute("SELECT * FROM madlibtestdata.mplda_doc_topic_dist_%d" % i)


    # compute similarites of plda/lda implementations against the word-topic and doc-topic distributions of the multiple plda_run and plda_label_test_documents runs
    # MADlib plda vs. Google lda / Google plda / R lda

    plpy.info("================================> Finished running, start to calculate Similarity.")

    sims = []

   
    glda_word_topic_dist_table_names = [] 
    gplda_word_topic_dist_table_names = []

    glda_doc_topic_dist_table_names = []
    gplda_doc_topic_dist_table_names = []
    rlda_doc_topic_dist_table_names = []


    if glda_word_topic_dist_table_name: glda_word_topic_dist_table_names = ["madlibtestdata.%s_%d"%(glda_word_topic_dist_table_name, i) for i in range(1,num_runs+1)]
    if gplda_word_topic_dist_table_name:gplda_word_topic_dist_table_names = ["madlibtestdata.%s_%d"%(gplda_word_topic_dist_table_name, i) for i in range(1,num_runs+1)]

    if glda_doc_topic_dist_table_name: glda_doc_topic_dist_table_names = ["madlibtestdata.%s_%d"%(glda_doc_topic_dist_table_name, i) for i in range(1,num_runs+1)]
    if gplda_doc_topic_dist_table_name: gplda_doc_topic_dist_table_names = ["madlibtestdata.%s_%d"%(gplda_doc_topic_dist_table_name, i) for i in range(1,num_runs+1)]
    if rlda_doc_topic_dist_table_name: rlda_doc_topic_dist_table_names = ["madlibtestdata.%s_%d"%(rlda_doc_topic_dist_table_name, i) for i in range(1,num_runs+1)] 

    acenter_dict  = { 'madlibtestdata.mplda_word_topic_dist_acenter': mplda_word_topic_dist_table_names, \
                      'madlibtestdata.glda_word_topic_dist_acenter' : glda_word_topic_dist_table_names, \
                      'madlibtestdata.gplda_word_topic_dist_acenter': gplda_word_topic_dist_table_names, \
                      'madlibtestdata.mplda_doc_topic_dist_acenter' : mplda_doc_topic_dist_table_names, \
                      'madlibtestdata.glda_doc_topic_dist_acenter'  : glda_doc_topic_dist_table_names, \
                      'madlibtestdata.gplda_doc_topic_dist_acenter' : gplda_doc_topic_dist_table_names, \
                      'madlibtestdata.rlda_doc_topic_dist_acenter'  : rlda_doc_topic_dist_table_names }

    for key, value in acenter_dict.items():
        if value is None: continue
        sql = " SELECT * FROM madlibtestdata.vplda_acenter(array ['%s']);" % "','".join(value)
        acenter = plpy.execute(sql)

        plpy.execute("DROP TABLE IF EXISTS %s" % key)
        plpy.execute("CREATE TABLE %s(id int, dist float[]);" % key)
        plan = plpy.prepare("INSERT INTO %s VALUES ($1, $2)" % key, ["INT", "FLOAT[]"])
        for r in acenter:
            plpy.execute(plan, [r['id'], r['dist']])


    for key, value in acenter_dict.items():
        if value is None: continue
        if key == 'madlibtestdata.mplda_word_topic_dist_acenter' or key == 'madlibtestdata.mplda_doc_topic_dist_acenter': continue
        if key == 'madlibtestdata.glda_word_topic_dist_acenter' or key == 'madlibtestdata.gplda_word_topic_dist_acenter': 
            sims_t = plpy.execute( "SELECT madlibtestdata.vplda_kl('madlibtestdata.mplda_word_topic_dist_acenter', '%s');" % key )
        if key == 'madlibtestdata.glda_doc_topic_dist_acenter' or key == 'madlibtestdata.gplda_doc_topic_dist_acenter' or key == 'madlibtestdata.rlda_doc_topic_dist_acenter':
            sims_t = plpy.execute( "SELECT madlibtestdata.vplda_kl('madlibtestdata.mplda_doc_topic_dist_acenter', '%s');" % key )
        sims.append(['mplda vs %s' % key.split('.')[1], sims_t[0]['vplda_kl']])

    # clean up after mutiple runs of plda_run and plda_label_test_documents
    plpy.execute( "DROP TABLE IF EXISTS %s;" % model_table )
    plpy.execute( "DROP TABLE IF EXISTS %s;" % train_output_data_table )
    plpy.execute( "DROP TABLE IF EXISTS %s;" % test_output_table )

    return sims
$$;

ALTER TYPE madlibtestdata.vplda_dist OWNER TO madlibtester;
ALTER TYPE madlibtestdata.vplda_similarity_result OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.vplda_acenter (table_array TEXT[]) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.vplda_stability( num_runs int, dict_table text, model_table text, num_topics int, alpha float, eta float,
                                                   train_corpus_table text, train_output_data_table text, train_num_iter int,
                                                   test_corpus_table text, test_output_table text ) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.vplda_similarity( num_runs int, dict_table text, model_table text, num_topics int, alpha float, eta float,
                                                    train_corpus_table text, train_output_data_table text, train_num_iter int,
                                                    test_corpus_table text, test_output_table text,
                                                    glda_word_topic_dist_table_names text, glda_doc_topic_dist_table_names text,
                                                    gplda_word_topic_dist_table_names text, gplda_doc_topic_dist_table_names text,
                                                    rlda_word_topic_dist_table_names text, rlda_doc_topic_dist_table_names text ) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.vplda_avgkl (table_array TEXT[]) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.vplda_kl( dist_table_name_1 text, dist_table_name_2 text ) OWNER TO madlibtester;
ALTER FUNCTION madlibtestdata.vplda_div( num1 float8, num2 float8, div_type text ) OWNER TO madlibtester;
-- negative_name_datatable
DROP TABLE IF EXISTS madlibtestdata.plda_sample_corpus;
CREATE TABLE madlibtestdata.plda_sample_corpus ( id int4, contents int4[] );
INSERT INTO madlibtestdata.plda_sample_corpus VALUES 
 (0, '{15,135,92,21,27,59,126,26,36,68}'),
 (1, '{162,67,129,122,169,193,211,129,121,184}'),
 (2, '{115,113,91,156,62,96,5,184,136,146}'),
 (3, '{24,82,114,34,43,87,76,188,3,154}'),
 (4, '{76,139,126,194,195,34,167,197,17,52}'),
 (5, '{65,144,240,31,297,281,109,167,97,86}'),
 (6, '{24,71,29,19,168,15,49,23,45,51}'),
 (7, '{188,28,150,100,164,114,56,191,65,136}'),
 (8, '{175,74,158,229,135,118,143,228,176,143}'),
 (9, '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_sample_dict;
CREATE TABLE madlibtestdata.plda_sample_dict ( id int, dict text[] );
insert into madlibtestdata.plda_sample_dict values 
 (1, '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_column_id_name_datatable
DROP TABLE IF EXISTS madlibtestdata.plda_invalid_column_id_name_corpus;
CREATE TABLE madlibtestdata.plda_invalid_column_id_name_corpus ( invalidid int4, contents int4[] );
INSERT INTO madlibtestdata.plda_invalid_column_id_name_corpus VALUES 
 (0, '{15,135,92,21,27,59,126,26,36,68}'),
 (1, '{162,67,129,122,169,193,211,129,121,184}'),
 (2, '{115,113,91,156,62,96,5,184,136,146}'),
 (3, '{24,82,114,34,43,87,76,188,3,154}'),
 (4, '{76,139,126,194,195,34,167,197,17,52}'),
 (5, '{65,144,240,31,297,281,109,167,97,86}'),
 (6, '{24,71,29,19,168,15,49,23,45,51}'),
 (7, '{188,28,150,100,164,114,56,191,65,136}'),
 (8, '{175,74,158,229,135,118,143,228,176,143}'),
 (9, '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_sample_dict;
CREATE TABLE madlibtestdata.plda_sample_dict ( id int, dict text[] );
insert into madlibtestdata.plda_sample_dict values 
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_column_id_datatype_datatable
DROP TABLE IF EXISTS madlibtestdata.plda_invalid_column_id_datatype_corpus;
CREATE TABLE madlibtestdata.plda_invalid_column_id_datatype_corpus ( id text, contents int4[] );
INSERT INTO madlibtestdata.plda_invalid_column_id_datatype_corpus VALUES 
 ('0', '{15,135,92,21,27,59,126,26,36,68}'),
 ('1', '{162,67,129,122,169,193,211,129,121,184}'),
 ('2', '{115,113,91,156,62,96,5,184,136,146}'),
 ('3', '{24,82,114,34,43,87,76,188,3,154}'),
 ('4', '{76,139,126,194,195,34,167,197,17,52}'),
 ('5', '{65,144,240,31,297,281,109,167,97,86}'),
 ('6', '{24,71,29,19,168,15,49,23,45,51}'),
 ('7', '{188,28,150,100,164,114,56,191,65,136}'),
 ('8', '{175,74,158,229,135,118,143,228,176,143}'),
 ('9', '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_sample_dict;
CREATE TABLE madlibtestdata.plda_sample_dict ( id int,dict text[] );
insert into madlibtestdata.plda_sample_dict values 
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_culumn_contents_name_datatable
DROP TABLE IF EXISTS madlibtestdata.plda_invalid_column_contents_name_corpus;
CREATE TABLE madlibtestdata.plda_invalid_column_contents_name_corpus ( id int4, invalidcontents int4[] );
INSERT INTO madlibtestdata.plda_invalid_column_contents_name_corpus VALUES
 (0, '{15,135,92,21,27,59,126,26,36,68}'),
 (1, '{162,67,129,122,169,193,211,129,121,184}'),
 (2, '{115,113,91,156,62,96,5,184,136,146}'),
 (3, '{24,82,114,34,43,87,76,188,3,154}'),
 (4, '{76,139,126,194,195,34,167,197,17,52}'),
 (5, '{65,144,240,31,297,281,109,167,97,86}'),
 (6, '{24,71,29,19,168,15,49,23,45,51}'),
 (7, '{188,28,150,100,164,114,56,191,65,136}'),
 (8, '{175,74,158,229,135,118,143,228,176,143}'),
 (9, '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_sample_dict;
CREATE TABLE madlibtestdata.plda_sample_dict ( id int,dict text[] );
insert into madlibtestdata.plda_sample_dict values
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_culumn_contents_datatype_datatable
DROP TABLE IF EXISTS madlibtestdata.plda_invalid_column_contents_datatype_corpus;
CREATE TABLE madlibtestdata.plda_invalid_column_contents_datatype_corpus ( id int4, contents text[] );
INSERT INTO madlibtestdata.plda_invalid_column_contents_datatype_corpus VALUES
 (0, '{"15","135","92","21","27","59","126","26","36","68"}'),
 (1, '{"162","67","129","122","169","193","211","129","121","184"}'),
 (2, '{"115","113","91","156","62","96","5","184","136","146"}'),
 (3, '{"24","82","114","34","43","87","76","188","3","154"}'),
 (4, '{"76","139","126","194","195","34","167","197","17","52"}'),
 (5, '{"65","144","240","31","297","281","109","167","97","86"}'),
 (6, '{"24","71","29","19","168","15","49","23","45","51"}'),
 (7, '{"188","28","150","100","164","114","56","191","65","136"}'),
 (8, '{"175","74","158","229","135","118","143","228","176","143"}'),
 (9, '{"6","4","28","69","17","24","70","90","72","44"}');


DROP TABLE IF EXISTS madlibtestdata.plda_sample_dict;
CREATE TABLE madlibtestdata.plda_sample_dict ( id int,dict text[] );
insert into madlibtestdata.plda_sample_dict values
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_name_dicttable
DROP TABLE IF EXISTS madlibtestdata.plda_sample_corpus;
CREATE TABLE madlibtestdata.plda_sample_corpus ( id int4, contents int4[] );
INSERT INTO madlibtestdata.plda_sample_corpus VALUES
 (0, '{15,135,92,21,27,59,126,26,36,68}'),
 (1, '{162,67,129,122,169,193,211,129,121,184}'),
 (2, '{115,113,91,156,62,96,5,184,136,146}'),
 (3, '{24,82,114,34,43,87,76,188,3,154}'),
 (4, '{76,139,126,194,195,34,167,197,17,52}'),
 (5, '{65,144,240,31,297,281,109,167,97,86}'),
 (6, '{24,71,29,19,168,15,49,23,45,51}'),
 (7, '{188,28,150,100,164,114,56,191,65,136}'),
 (8, '{175,74,158,229,135,118,143,228,176,143}'),
 (9, '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_sample_dict;
CREATE TABLE madlibtestdata.plda_sample_dict ( id int,dict text[] );
insert into madlibtestdata.plda_sample_dict values
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_column_dict_name_dicttable
DROP TABLE IF EXISTS madlibtestdata.plda_sample_corpus;
CREATE TABLE madlibtestdata.plda_sample_corpus ( id int4, contents int4[] );
INSERT INTO madlibtestdata.plda_sample_corpus VALUES
 (0, '{15,135,92,21,27,59,126,26,36,68}'),
 (1, '{162,67,129,122,169,193,211,129,121,184}'),
 (2, '{115,113,91,156,62,96,5,184,136,146}'),
 (3, '{24,82,114,34,43,87,76,188,3,154}'),
 (4, '{76,139,126,194,195,34,167,197,17,52}'),
 (5, '{65,144,240,31,297,281,109,167,97,86}'),
 (6, '{24,71,29,19,168,15,49,23,45,51}'),
 (7, '{188,28,150,100,164,114,56,191,65,136}'),
 (8, '{175,74,158,229,135,118,143,228,176,143}'),
 (9, '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_invalid_column_dict_name_dict;
CREATE TABLE madlibtestdata.plda_invalid_column_dict_name_dict ( id int,invaliddict text[] );
insert into madlibtestdata.plda_invalid_column_dict_name_dict values
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_column_dict_datatype_dicttable
DROP TABLE IF EXISTS madlibtestdata.plda_sample_corpus;
CREATE TABLE madlibtestdata.plda_sample_corpus ( id int4, contents int4[] );
INSERT INTO madlibtestdata.plda_sample_corpus VALUES
 (0, '{15,135,92,21,27,59,126,26,36,68}'),
 (1, '{162,67,129,122,169,193,211,129,121,184}'),
 (2, '{115,113,91,156,62,96,5,184,136,146}'),
 (3, '{24,82,114,34,43,87,76,188,3,154}'),
 (4, '{76,139,126,194,195,34,167,197,17,52}'),
 (5, '{65,144,240,31,297,281,109,167,97,86}'),
 (6, '{24,71,29,19,168,15,49,23,45,51}'),
 (7, '{188,28,150,100,164,114,56,191,65,136}'),
 (8, '{175,74,158,229,135,118,143,228,176,143}'),
 (9, '{6,4,28,69,17,24,70,90,72,44}');


DROP TABLE IF EXISTS madlibtestdata.plda_invalid_column_dict_datatype_dict;
CREATE TABLE madlibtestdata.plda_invalid_column_dict_datatype_dict ( id int,dict int4[] );
insert into madlibtestdata.plda_invalid_column_dict_datatype_dict values
 (1,'{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297}');

-- negative_existing_modeltable
DROP TABLE IF EXISTS madlibtestdata.plda_existing_out_model;
CREATE TABLE madlibtestdata.plda_existing_out_model ( id int4, contents int4[] );

-- negative_existing_outputdatatable
DROP TABLE IF EXISTS madlibtestdata.plda_existing_out_corpus;
CREATE TABLE madlibtestdata.plda_existing_out_corpus ( id int4, contents int4[] );

-- negative_name_test_table

-- negative_column_id_name_test_table

-- negative_column_id_datatype_test_table

-- negative_column_contents_name_test_table

-- negative_column_contents_datatype_test_table

-- negative_existing_out_labeling
DROP TABLE IF EXISTS madlibtestdata.plda_existing_out_labeling;
CREATE TABLE madlibtestdata.plda_existing_out_labeling ( id int4, contents int4[] );

-- negative_name_model_table

-- negative_column_iternum_name_model_table

-- negative_column_iternum_datatype_model_table

-- negative_column_gcounts_name_model_table

-- negative_column_gcounts_datatype_model_table

-- negative_column_tcounts_name_model_table

-- negative_column_tcounts_datatype_model_table

-- label_negative_name_dict_table

-- label_negative_column_dict_name_dict_table

-- label_negative_column_dict_datatype_dict_table

-- label_negative_alpha

-- label_negative_eta

-- run_empty_test_table
DROP TABLE IF EXISTS madlibtestdata.plda_sample_empty_corpus;
CREATE TABLE madlibtestdata.plda_sample_empty_corpus ( id int4, contents int4[] );

-- run_empty_dict_table
DROP TABLE IF EXISTS madlibtestdata.plda_sample_empty_dict;
CREATE TABLE madlibtestdata.plda_sample_empty_dict ( id int,dict text[] );

-- label_empty_corpus_table


-- change owner of datasets from gpamdin to madlibtester
ALTER TABLE madlibtestdata.plda_existing_out_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_existing_out_labeling OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_existing_out_model OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_invalid_column_contents_datatype_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_invalid_column_contents_name_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_invalid_column_dict_datatype_dict OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_invalid_column_dict_name_dict OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_invalid_column_id_datatype_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_invalid_column_id_name_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_sample_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_sample_dict OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_sample_empty_corpus OWNER TO madlibtester;
ALTER TABLE madlibtestdata.plda_sample_empty_dict OWNER TO madlibtester;

