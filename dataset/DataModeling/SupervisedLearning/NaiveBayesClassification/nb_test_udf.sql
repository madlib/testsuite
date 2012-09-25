CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_prepared_data_tables( training_source TEXT,
                                                                               training_class_column TEXT,
                                                                               training_attr_column TEXT,
                                                                               trained_probs_name TEXT,
                                                                               trained_priors_name TEXT)
RETURNS VOID AS $$
DECLARE
    num_attrs   INT;
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS ' || trained_probs_name || ' CASCADE;';
    EXECUTE 'DROP TABLE IF EXISTS ' || trained_priors_name || ' CASCADE;';

    stmt = 'SELECT max(array_upper(' || training_attr_column || ',1)) FROM ' || training_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO num_attrs;

    stmt = 'SELECT madlib.create_nb_prepared_data_tables(''' || training_source || ''',' ||
                                                         '''' || training_class_column || ''',' ||
                                                         '''' || training_attr_column || ''',' ||
                                                         num_attrs || ',' ||
                                                         '''' || trained_probs_name || ''',' ||
                                                         '''' || trained_priors_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_classify_view( trained_probs_name TEXT,
                                                                        trained_priors_name TEXT,
                                                                        classify_source TEXT,
                                                                        classify_key_column TEXT,
                                                                        classify_attr_column TEXT,
                                                                        classified_result_name TEXT)
RETURNS VOID AS $$
DECLARE
    num_attrs   INT;
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP VIEW IF EXISTS ' || classified_result_name || ' CASCADE;';

    stmt = 'SELECT max(array_upper(' || classify_attr_column || ',1)) FROM ' || classify_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO num_attrs;

    stmt = 'SELECT madlib.create_nb_classify_view(''' || trained_probs_name || ''',' ||
                                                  '''' || trained_priors_name || ''',' ||
                                                  '''' || classify_source || ''',' ||
                                                  '''' || classify_key_column || ''',' ||
                                                  '''' || classify_attr_column || ''',' ||
                                                  num_attrs || ',' ||
                                                  '''' || classified_result_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_classify_view_table( training_source TEXT,
                                                                              training_class_column TEXT,
                                                                              training_attr_column TEXT,
                                                                              classify_source TEXT,
                                                                              classify_key_column TEXT,
                                                                              classify_attr_column TEXT,
                                                                              classified_result_name TEXT)
RETURNS VOID AS $$
DECLARE
    num_attrs   INT;
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP VIEW IF EXISTS ' || classified_result_name || ' CASCADE;';

    stmt = 'SELECT max(array_upper(' || training_attr_column || ',1)) FROM ' || training_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO num_attrs;

    stmt = 'SELECT madlib.create_nb_classify_view(''' || training_source || ''',' ||
                                                  '''' || training_class_column || ''',' ||
                                                  '''' || training_attr_column || ''',' ||
                                                  '''' || classify_source || ''',' ||
                                                  '''' || classify_key_column || ''',' ||
                                                  '''' || classify_attr_column || ''',' ||
                                                  num_attrs || ',' ||
                                                  '''' || classified_result_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_probs_view( trained_probs_name TEXT,
                                                                     trained_priors_name TEXT,
                                                                     classify_source TEXT,
                                                                     classify_key_column TEXT,
                                                                     classify_attr_column TEXT,
                                                                     classified_probs_name TEXT)
RETURNS VOID AS $$
DECLARE
    num_attrs   INT;
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP VIEW IF EXISTS ' || classified_probs_name || ' CASCADE;';

    stmt = 'SELECT max(array_upper(' || classify_attr_column || ',1)) FROM ' || classify_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO num_attrs;

    stmt = 'SELECT madlib.create_nb_probs_view(''' || trained_probs_name || ''',' ||
                                               '''' || trained_priors_name || ''',' ||
                                               '''' || classify_source || ''',' ||
                                               '''' || classify_key_column || ''',' ||
                                               '''' || classify_attr_column || ''',' ||
                                               num_attrs || ',' ||
                                               '''' || classified_probs_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_probs_view_table( training_source TEXT,
                                                                           training_class_column TEXT,
                                                                           training_attr_column TEXT,
                                                                           classify_source TEXT,
                                                                           classify_key_column TEXT,
                                                                           classify_attr_column TEXT,
                                                                           classified_probs_name TEXT)
RETURNS VOID AS $$
DECLARE
    num_attrs   INT;
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP VIEW IF EXISTS ' || classified_probs_name || ' CASCADE;';

    stmt = 'SELECT max(array_upper(' || training_attr_column || ',1)) FROM ' || training_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO num_attrs;

    stmt = 'SELECT madlib.create_nb_probs_view(''' || training_source || ''',' ||
                                               '''' || training_class_column || ''',' ||
                                               '''' || training_attr_column || ''',' ||
                                               '''' || classify_source || ''',' ||
                                               '''' || classify_key_column || ''',' ||
                                               '''' || classify_attr_column || ''',' ||
                                               num_attrs || ',' ||
                                               '''' || classified_probs_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_classify_fn( trained_probs_name TEXT,
                                                                      trained_priors_name TEXT,
                                                                      num_attrs INT,
                                                                      classified_func_name TEXT)
RETURNS VOID AS $$
DECLARE
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP FUNCTION IF EXISTS ' || classified_func_name || '(INT[], FLOAT8);';

    stmt = 'SELECT madlib.create_nb_classify_fn(''' || trained_probs_name || ''',' ||
                                                '''' || trained_priors_name || ''',' ||
                                                num_attrs || ',' ||
                                                '''' || classified_func_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_classify_fn_table( training_source TEXT,
                                                                            training_class_column TEXT,
                                                                            training_attr_column TEXT,
                                                                            classified_func_name TEXT)
RETURNS VOID AS $$
DECLARE
    num_attrs   INT;
    stmt        TEXT;
BEGIN
    EXECUTE 'DROP FUNCTION IF EXISTS ' || classified_func_name || '(INT[], FLOAT8);';

    stmt = 'SELECT max(array_upper(' || training_attr_column || ',1)) FROM ' || training_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO num_attrs;

    stmt = 'SELECT madlib.create_nb_classify_fn(''' || training_source || ''',' ||
                                                '''' || training_class_column || ''',' ||
                                                '''' || training_attr_column || ''',' ||
                                                num_attrs || ',' ||
                                                '''' || classified_func_name || ''');';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_classify_fn_revoke( classify_source TEXT,
                                                                             classify_attr_column TEXT,
                                                                             classified_func_name TEXT,
                                                                             smoothing_factor FLOAT8)
RETURNS INT[] AS $$
DECLARE
    stmt        TEXT;
    class       INT[] :='{}';
BEGIN
    stmt = 'SELECT ' || classified_func_name  || '(' || classify_attr_column || ',' || smoothing_factor || ') FROM ' || classify_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO class;

    RETURN class;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION madlibtestdata.test_create_nb_classify_fn_revoke_workaround( classify_source TEXT,
                                                                                        classify_key_column TEXT,
                                                                                        classify_attr_column TEXT,
                                                                                        classified_func_name TEXT,
                                                                                        smoothing_factor FLOAT8)
RETURNS INT AS $$
DECLARE
    attr        TEXT;
    stmt        TEXT;
    class       INT :=-1;
BEGIN
    stmt = 'SELECT ''{'' || array_to_string(' || classify_attr_column || ','','') || ''}'' FROM ' || classify_source || ' ORDER BY ' || classify_key_column || ' LIMIT 1;';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO attr;

    stmt = 'SELECT (' || classified_func_name  || '(''' || attr || ''',' || smoothing_factor || '))[1];';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO class;

    RETURN class;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION madlibtestdata.test_calculate_accuracy( classify_source TEXT,
                                                                   classify_key_column TEXT,
                                                                   classify_class_column TEXT,
                                                                   classified_result_name TEXT)
RETURNS DECIMAL(16,6) AS $$
DECLARE
    stmt        TEXT;
    correct     INT :=0;
    total       INT :=0;
    accuracy    DECIMAL(16,6) :=0.0;
BEGIN
    stmt = 'SELECT count(*) FROM ' || classify_source || ';';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO total;

    stmt = 'SELECT count(*) ' ||
           'FROM ' || classify_source || ' exp, ' || classified_result_name || ' act ' ||
           'WHERE exp.' || classify_key_column || '=act.key AND exp.' || classify_class_column || '=act.nb_classification[1];';
    RAISE INFO '%', stmt;
    EXECUTE stmt INTO correct;

    IF total > 0 THEN
        accuracy := (correct * 1.0) / (total * 1.0);
    END IF;

    RETURN accuracy;
END
$$ LANGUAGE plpgsql;


ALTER FUNCTION madlibtestdata.test_create_nb_prepared_data_tables( training_source TEXT,
                                                           training_class_column TEXT,
                                                           training_attr_column TEXT,
                                                           trained_probs_name TEXT,
                                                           trained_priors_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_classify_view( trained_probs_name TEXT,
                                                            trained_priors_name TEXT,
                                                            classify_source TEXT,
                                                            classify_key_column TEXT,
                                                            classify_attr_column TEXT,
                                                            classified_result_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_classify_view_table( training_source TEXT,
                                                                  training_class_column TEXT,
                                                                  training_attr_column TEXT,
                                                                  classify_source TEXT,
                                                                  classify_key_column TEXT,
                                                                  classify_attr_column TEXT,
                                                                  classified_result_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_probs_view( trained_probs_name TEXT,
                                                         trained_priors_name TEXT,
                                                         classify_source TEXT,
                                                         classify_key_column TEXT,
                                                         classify_attr_column TEXT,
                                                         classified_probs_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_probs_view_table( training_source TEXT,
                                                               training_class_column TEXT,
                                                               training_attr_column TEXT,
                                                               classify_source TEXT,
                                                               classify_key_column TEXT,
                                                               classify_attr_column TEXT,
                                                               classified_probs_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_classify_fn( trained_probs_name TEXT,
                                                          trained_priors_name TEXT,
                                                          num_attrs INT,
                                                          classified_func_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_classify_fn_table( training_source TEXT,
                                                                training_class_column TEXT,
                                                                training_attr_column TEXT,
                                                                classified_func_name TEXT) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_classify_fn_revoke( classify_source TEXT,
                                                                 classify_attr_column TEXT,
                                                                 classified_func_name TEXT,
                                                                 smoothing_factor FLOAT8) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_create_nb_classify_fn_revoke_workaround( classify_source TEXT,
                                                                            classify_key_column TEXT,
                                                                            classify_attr_column TEXT,
                                                                            classified_func_name TEXT,
                                                                            smoothing_factor FLOAT8) OWNER TO madlibtester;

ALTER FUNCTION madlibtestdata.test_calculate_accuracy( classify_source TEXT,
                                                       classify_key_column TEXT,
                                                       classify_class_column TEXT,
                                                       classified_result_name TEXT) OWNER TO madlibtester;

