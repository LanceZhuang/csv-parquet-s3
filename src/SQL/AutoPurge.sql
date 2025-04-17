-- Ensure output is displayed in SQL*Plus
SET SERVEROUTPUT ON;

DECLARE
    -- Parameter to control debug mode (TRUE for debug with context, FALSE for normal execution)
    tables_to_process VARCHAR2(1000) := '&1';
    debug BOOLEAN := FALSE;  -- Set to TRUE for debug mode, FALSE for normal execution
    v_index_name VARCHAR2(255);
    owner_name VARCHAR2(255) := NVL('&2', 'RUNHFOG'); -- Ownername/username, defaults to RUNHFOG
    v_toRollback BOOLEAN := FALSE; -- Controls rollback/commit

    -- Sample data structure to hold the parameters for each table
    TYPE table_record IS RECORD (
                                    user_name         VARCHAR2(30),
                                    table_name        VARCHAR2(30),
                                    action            VARCHAR2(10),
                                    sqlstatement      VARCHAR2(4000),
                                    max_record_count  NUMBER
                                );

    -- Collection of table records
    TYPE table_array IS TABLE OF table_record;
    tables table_array;

    v_sql VARCHAR2(4000);
    v_table_name VARCHAR2(300);
    v_user_name VARCHAR2(300);
    v_action VARCHAR2(300);
    v_sqlstatement VARCHAR2(2000);
    v_constraint_name VARCHAR2(2000);
    v_found BOOLEAN;
    v_success_flag BOOLEAN;
    v_param_list DBMS_UTILITY.LNAME_ARRAY;
    v_param_count INTEGER;
    v_run_all BOOLEAN := FALSE;
    v_is_in_list BOOLEAN := FALSE;
    v_table_size VARCHAR2(100);
    v_savepoint_name VARCHAR2(30) := 'before_delete';
    v_max_record_count NUMBER;
    v_record_count NUMBER;
    v_parallel_degree NUMBER;

    -- Function to validate table existence
    FUNCTION ValidateTable(p_user_name VARCHAR2, p_table_name VARCHAR2) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM all_tables
        WHERE owner = UPPER(p_user_name)
          AND table_name = UPPER(p_table_name);

        RETURN v_count > 0;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END ValidateTable;

    -- Procedure to log messages into the Purge_Audit table
    PROCEDURE LogToAudit(p_timestamp VARCHAR2, p_user_name VARCHAR2, p_table_name VARCHAR2, p_log_message VARCHAR2) IS
    BEGIN
        INSERT INTO Purge_Audit (log_timestamp, log_message, user_name, table_name)
        VALUES (TO_TIMESTAMP(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF'), p_log_message, p_user_name, p_table_name);
        COMMIT;
    END LogToAudit;

    PROCEDURE InitializeTables IS
    BEGIN
        SELECT user_name, TRIM(table_name), action, sqlstatement, max_record_count
                BULK COLLECT INTO tables
        FROM PURGE_CONFIG;
    END InitializeTables;

    FUNCTION ExecuteSQL(p_sql VARCHAR2, p_user_name VARCHAR2, p_table_name VARCHAR2, p_rollback BOOLEAN := FALSE) RETURN NUMBER IS
    BEGIN
        v_record_count := 0;
        IF debug THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' DEBUG: ' || p_sql);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'DEBUG: ' || p_sql);
        ELSE
            SAVEPOINT RB_POINT;
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Savepoint created for potential rollback.');
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Savepoint created for potential rollback.');
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' Executing SQL: ' || p_sql);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Executing SQL: ' || p_sql);
            EXECUTE IMMEDIATE p_sql;
            v_record_count := SQL%ROWCOUNT;
            IF p_rollback THEN
                ROLLBACK TO RB_POINT;
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Rollback executed.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Rollback executed.');
            END IF;
        END IF;
        RETURN v_record_count;
    END ExecuteSQL;

    PROCEDURE ProcessTable(p_table_name VARCHAR2, p_user_name VARCHAR2, p_action VARCHAR2, p_sqlstatement VARCHAR2, p_max_record_count NUMBER) IS
        v_matching_record_count NUMBER;
    BEGIN
        -- Validate table existence
        IF NOT ValidateTable(p_user_name, p_table_name) THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Table ' || p_table_name || ' does not exist for user ' || p_user_name || '. Skipping.');
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Table does not exist. Skipping.');
            RETURN;
        END IF;

        -- Log table stats before purge
        v_sql := 'SELECT num_rows, blocks*8/1024/1024 AS size_Gb ' ||
                 'FROM all_tables ' ||
                 'WHERE owner = UPPER(''' || p_user_name || ''') AND table_name = UPPER(''' || p_table_name || ''')';
        EXECUTE IMMEDIATE v_sql INTO v_record_count, v_table_size;
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': TABLE STATS BEFORE PURGE: ' || p_table_name || ' - Size: ' || v_table_size || ' GB, Records: ' || v_record_count);
        LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'TABLE STATS BEFORE PURGE: Size: ' || v_table_size || ' GB, Records: ' || v_record_count);

        -- Determine parallel degree based on table size
        v_parallel_degree := CASE
                                 WHEN v_table_size < 1 THEN 4
                                 WHEN v_table_size < 10 THEN 8
                                 ELSE 16
            END;

        -- Check if action is NULL or empty
        IF p_action IS NULL OR TRIM(p_action) = '' THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': No action specified for ' || p_table_name || '. Skipping.');
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'No action specified. Skipping.');
            RETURN;
        END IF;

        IF p_action = 'SQL' THEN
            v_sql := p_sqlstatement;
            -- Check for TRUNCATE to optimize performance
            IF UPPER(v_sql) LIKE 'TRUNCATE%' THEN
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Executing TRUNCATE for ' || p_table_name);
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Executing TRUNCATE');
                v_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name, v_toRollback);
            ELSE
                v_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name, v_toRollback);
            END IF;
        ELSIF p_action = 'CRITERIA' THEN
            -- Get record count matching criteria
            v_sql := 'SELECT COUNT(*) /*+ PARALLEL(' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || ', ' || v_parallel_degree || ') */ FROM ' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || p_sqlstatement;
            v_matching_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name);

            -- Check if record count exceeds maximum
            IF v_matching_record_count > p_max_record_count THEN
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Record count (' || v_matching_record_count || ') exceeds maximum (' || p_max_record_count || ') for ' || p_table_name || '. Aborted.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Record count (' || v_matching_record_count || ') exceeds maximum (' || p_max_record_count || '). Aborted.');
                RETURN;
            END IF;

            -- Disable foreign key constraints
            FOR constraint_rec IN (
                SELECT constraint_name, table_name
                FROM user_constraints
                WHERE r_constraint_name IN (
                    SELECT constraint_name
                    FROM user_constraints
                    WHERE table_name = UPPER(p_table_name)
                      AND constraint_type = 'P'
                )
                  AND constraint_type = 'R'
                ) LOOP
                    v_constraint_name := constraint_rec.constraint_name;
                    v_sql := 'ALTER TABLE ' || constraint_rec.table_name || ' DISABLE CONSTRAINT ' || v_constraint_name;
                    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Disabling constraint ' || v_constraint_name);
                    EXECUTE IMMEDIATE v_sql;
                    LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Disabled constraint ' || v_constraint_name);
                END LOOP;

            -- Execute parallel delete
            v_sql := 'DELETE /*+ PARALLEL(' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || ', ' || v_parallel_degree || ') */ FROM ' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || p_sqlstatement;
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Deleting records from ' || p_table_name);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Deleting records');
            v_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name, v_toRollback);

            -- Re-enable foreign key constraints
            FOR constraint_rec IN (
                SELECT constraint_name, table_name
                FROM user_constraints
                WHERE r_constraint_name IN (
                    SELECT constraint_name
                    FROM user_constraints
                    WHERE table_name = UPPER(p_table_name)
                      AND constraint_type = 'P'
                )
                  AND constraint_type = 'R'
                ) LOOP
                    v_sql := 'ALTER TABLE ' || constraint_rec.table_name || ' ENABLE CONSTRAINT ' || constraint_rec.constraint_name;
                    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Enabling constraint ' || constraint_rec.constraint_name);
                    EXECUTE IMMEDIATE v_sql;
                    LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Enabled constraint ' || constraint_rec.constraint_name);
                END LOOP;

            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Delete completed for ' || p_table_name || '. Records deleted: ' || v_record_count);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Delete completed. Records deleted: ' || v_record_count);

            IF NOT v_toRollback THEN
                COMMIT;
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Committed deletions.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Committed deletions.');
            END IF;
        END IF;
        -- Recommendation: Consider partitioning large tables to improve delete performance.
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Error in ProcessTable for ' || p_table_name || ': ' || SQLERRM);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Error: ' || SQLERRM);
            RAISE;
    END ProcessTable;

BEGIN
    -- Initialize table records
    InitializeTables;

    -- Remove spaces from parameter
    tables_to_process := REPLACE(tables_to_process, ' ', '');

    -- Check if parameter is 'all' or empty
    IF TRIM(tables_to_process) IS NULL OR UPPER(tables_to_process) = 'ALL' THEN
        v_run_all := TRUE;
    ELSE
        DBMS_UTILITY.COMMA_TO_TABLE(tables_to_process, v_param_count, v_param_list);
    END IF;

    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Start Auto Purge with parameters: ' || tables_to_process || ', Owner: ' || owner_name);
    LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), NULL, NULL, 'Start Auto Purge with parameters: ' || tables_to_process || ', Owner: ' || owner_name);
    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Table count: ' || tables.COUNT);

    -- Loop through tables
    FOR i IN 1 .. tables.COUNT LOOP
            v_table_name := tables(i).table_name;
            v_user_name := NVL(tables(i).user_name, owner_name); -- Use owner_name if user_name is null
            v_action := tables(i).action;
            v_sqlstatement := tables(i).sqlstatement;
            v_max_record_count := tables(i).max_record_count;
            v_is_in_list := FALSE;

            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Checking: ' || v_table_name || ' - ' || v_user_name || ' - ' || v_action);

            IF v_run_all THEN
                v_is_in_list := TRUE;
            ELSE
                FOR j IN 1 .. v_param_count LOOP
                        IF UPPER(v_table_name) = UPPER(v_param_list(j)) THEN
                            v_is_in_list := TRUE;
                            EXIT;
                        END IF;
                    END LOOP;
            END IF;

            IF v_is_in_list THEN
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Processing: ' || v_table_name || ' - ' || v_user_name || ' - ' || v_action);
                ProcessTable(v_table_name, v_user_name, v_action, v_sqlstatement, v_max_record_count);
            END IF;
        END LOOP;

    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Completed for all tables.');
    LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), NULL, NULL, 'Completed for all tables.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Fatal error: ' || SQLERRM || ' at ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), NULL, NULL, 'Fatal error: ' || SQLERRM || ' at ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END;
/