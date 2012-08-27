CREATE OR REPLACE FUNCTION madlibtestdata.c45_cross_validate(split_criterion TEXT, training_set TEXT, validation_table_name TEXT, 
    continuous_feature_names TEXT, feature_col_names TEXT, 
    id_col_name TEXT, class_col_name TEXT, confidence_level FLOAT, how2handle_missing_value TEXT, max_tree_depth INT,
    node_prune_threshold FLOAT, node_split_threshold FLOAT, verbosity INT,
    subset_number FLOAT)

RETURNS FLOAT8 AS $$
declare
    index                INT :=0;
    accuracy             float8 :=0;
    true_train_set       TEXT;
    true_validation_set  TEXT;
    true_forest          TEXT;
    stmt                 TEXT;
    score                float8 :=0;
begin
    true_train_set = training_set||'_train';
    true_validation_set = training_set||'_validate';
    true_forest = training_set || '_forest';
    drop table if exists auxiliary_info;
    stmt ='create temp table auxiliary_info as select id,random() as rand from '||training_set||
        ' distributed by (id);';
    RAISE INFO '%', stmt;
    EXECUTE stmt;
    FOR index in 0..subset_number-1 LOOP
        EXECUTE 'DROP TABLE IF EXISTS '||true_train_set||index||';';
        EXECUTE 'DROP TABLE IF EXISTS '||true_validation_set||index||';';
        EXECUTE 'DROP TABLE IF EXISTS '||true_forest||index||';';

        stmt ='CREATE TABLE '||true_validation_set||index||' AS select n1.* from '
            ||training_set||' n1, auxiliary_info n2 where n1.id=n2.id and n2.rand>='
            ||index/subset_number||' and n2.rand<'||(index+1)/subset_number||';';
        RAISE INFO '%', stmt;
        EXECUTE stmt;
        stmt = 'CREATE TABLE '||true_train_set||index||' AS select n1.* from '
            ||training_set||' n1, auxiliary_info n2 where n1.id=n2.id and (n2.rand<'
            ||index/subset_number||' or n2.rand>='||(index+1)/subset_number||');';
        RAISE INFO '%', stmt;
        EXECUTE stmt;
        RAISE INFO 'trace -------begin';
        RAISE INFO '%', split_criterion;
        RAISE INFO '%', true_train_set;
        RAISE INFO '%', true_forest;
        RAISE INFO '%', validation_table_name;
         
        stmt = 'SELECT * FROM madlib.c45_train('''
            ||split_criterion||''','''
            ||true_train_set||index||''','''
            ||true_forest||index||''',';
        
        IF validation_table_name IS NOT NULL THEN
            stmt = stmt||''''|| validation_table_name ||''','; 
        ELSE
            stmt = stmt||'NULL,';
        END IF;

        RAISE INFO '%', stmt;
        IF continuous_feature_names IS NOT NULL THEN
            stmt = stmt||''''||continuous_feature_names||''',';
        ELSE
            stmt = stmt||'NULL,';
        END IF;
        RAISE INFO '%', stmt;
        IF feature_col_names IS NOT NULL THEN
            stmt = stmt||''''||feature_col_names||''',';
        ELSE
            stmt = stmt||'NULL,';
        END IF;
        RAISE INFO '%', stmt;
        stmt = stmt||''''
            ||id_col_name||''','''
            ||class_col_name||''','
            ||confidence_level||','''
            ||how2handle_missing_value||''','
            ||max_tree_depth||','
            ||node_prune_threshold||','
            ||node_split_threshold||','
            ||verbosity||');';

        RAISE INFO '%', stmt;
        EXECUTE stmt;
        stmt = 'SELECT * FROM madlib.c45_score('''||true_forest||index||''','''||true_validation_set||index||''',0);';
        RAISE INFO '%', stmt;
        EXECUTE stmt INTO score;
        stmt = 'SELECT * FROM madlib.c45_clean('''||true_forest||index||''');';
        RAISE INFO '%', stmt;
        EXECUTE stmt;

        accuracy = accuracy + score;

    END LOOP;
    accuracy= accuracy/subset_number;
    RETURN accuracy;
end
$$ language plpgsql;

ALTER FUNCTION madlibtestdata.c45_cross_validate(split_criterion TEXT, training_set TEXT, validation_table_name TEXT,
    continuous_feature_names TEXT, feature_col_names TEXT,
    id_col_name TEXT, class_col_name TEXT, confidence_level FLOAT, how2handle_missing_value TEXT, max_tree_depth INT,
    node_prune_threshold FLOAT, node_split_threshold FLOAT, verbosity INT,
    subset_number FLOAT) OWNER TO madlibtester;

