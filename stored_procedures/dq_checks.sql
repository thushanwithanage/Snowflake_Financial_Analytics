CREATE OR REPLACE PROCEDURE curated.run_dq_checks()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    c1 CURSOR FOR
        SELECT table_name,
               column_name,
               check_type,
               check_params,
               severity
        FROM curated.dq_rules
        WHERE active = TRUE;

    v_table_name   STRING;
    v_column_name  STRING;
    v_check_type   STRING;
    v_check_params STRING;
    v_severity     STRING;
    condition_sql  STRING;
    final_sql      STRING;

BEGIN
    FOR record IN c1 DO
        v_table_name   := record.table_name;
        v_column_name  := record.column_name;
        v_check_type   := record.check_type;
        v_check_params := record.check_params;
        v_severity     := record.severity;

        IF (v_check_type = 'NULL_CHECK') THEN
            condition_sql := v_column_name || ' IS NULL';
        ELSEIF (v_check_type = 'RANGE_CHECK') THEN
            condition_sql := 'NOT (' || v_column_name || ' ' || v_check_params || ')';
        ELSEIF (v_check_type = 'FORMAT_CHECK') THEN
            condition_sql := 'NOT (' || v_column_name || ' ' || v_check_params || ')';
        END IF;

        final_sql := '
            INSERT INTO curated.dq_logs
                (table_name, column_name, check_type, issue_description, row_count, severity)
            SELECT
                ''' || v_table_name  || ''',
                ''' || v_column_name || ''',
                ''' || v_check_type  || ''',
                ''' || v_column_name || ' failed ' || v_check_type || ''',
                COUNT(*),
                ''' || v_severity    || '''
            FROM ' || v_table_name || '
            WHERE ' || condition_sql || '
            HAVING COUNT(*) > 0
        ';

        EXECUTE IMMEDIATE :final_sql;

    END FOR;

    RETURN 'DQ checks completed successfully';
END;
$$;

CALL curated.run_dq_checks();