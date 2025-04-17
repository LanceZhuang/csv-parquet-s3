-- Ensure output is displayed in SQL*Plus
SET SERVEROUTPUT ON;

DECLARE
    -- Parameter to control debug mode (TRUE for debug with context, FALSE for normal execution)
    tables_to_process VARCHAR2(1000);
    debug BOOLEAN := FALSE;  -- Set to TRUE for debug mode, FALSE for normal execution
    v_index_name VARCHAR2(255);
    owner_name VARCHAR2(255) := 'RUNHFOG'; -- DEFAULT Ownername/username for all tables
    v_toRollback BOOLEAN := FALSE; -- New variable to control rollback/commit

    -- Sample data structure to hold the parameters for each table
    TYPE table_record IS RECORD (
                                    user_name         VARCHAR2(30),
                                    table_name        VARCHAR2(30),
                                    action            VARCHAR2(10),
                                    sqlstatement      VARCHAR2(4000),
                                    max_record_count  NUMBER  -- New column for maximum record count
                                );

    -- Collection of table records
    TYPE table_array IS TABLE OF table_record;
    tables table_array; -- Declare the array without initialization

    v_sql VARCHAR2(4000);
    v_table_name VARCHAR2(300);
    v_user_name VARCHAR2(300);
    v_action VARCHAR2(300);
    v_sqlstatement varchar2(2000);
    v_constraint_name varchar2(2000);
    v_found BOOLEAN;
    v_success_flag BOOLEAN;
    v_param_list DBMS_UTILITY.LNAME_ARRAY; -- Array to hold parameters
    v_param_count INTEGER; -- Counter for number of parameters
    v_run_all BOOLEAN := FALSE; -- Flag to determine if all tables should be processed
    v_is_in_list BOOLEAN := FALSE; -- Flag to check if current table is in the provided list
    v_table_size VARCHAR2(100);
    v_savepoint_name VARCHAR2(30) := 'before_delete'; -- Savepoint name for rollback
    v_max_record_count NUMBER; -- New variable to hold the maximum record count
    v_record_count NUMBER;     -- Variable to hold the record count for delete sql statement

    ind NUMBER;              -- Loop index
    spos NUMBER;             -- String starting position
    slen NUMBER;             -- String length for output
    h1           NUMBER;
    h1_dir       VARCHAR2 (20) := 'DATA_PUMP_DIR'; -- Data Pump Directory
    h1_dir_log   VARCHAR2 (20) := 'DATA_PUMP_DIR'; -- Data Pump Directory
    percent_done NUMBER;     -- Percentage of job complete
    job_state VARCHAR2(30);  -- To keep track of job state
    le ku$_LogEntry;         -- For WIP and error messages
    js ku$_JobStatus;        -- The job status from get_status
    jd ku$_JobDesc;          -- The job description from get_status
    sts ku$_Status;          -- The status object returned by get_status

    -- Procedure to log messages into the Purge_Audit table
    PROCEDURE LogToAudit(p_timestamp VARCHAR2, p_user_name VARCHAR2, p_table_name VARCHAR2, p_log_message VARCHAR2) IS
    BEGIN
        INSERT INTO Purge_Audit (log_timestamp, log_message, user_name, table_name)
        VALUES (TO_TIMESTAMP(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF'), p_log_message, p_user_name, p_table_name);
        COMMIT; -- Commit the transaction to ensure the log is saved
    END LogToAudit;

    PROCEDURE InitializeTables IS
    BEGIN
        SELECT user_name, trim(table_name), action, sqlstatement, max_record_count
                BULK COLLECT INTO tables
        FROM PURGE_CONFIG;  -- Read values from the configuration table
    END InitializeTables;

    FUNCTION ExecuteSQL(p_sql VARCHAR2, p_user_name VARCHAR2, p_table_name VARCHAR2, p_rollback BOOLEAN:=FALSE) RETURN NUMBER IS
    BEGIN
        v_record_count := 0;
        IF debug THEN
            -- Log the SQL statement without executing it
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' DEBUG: ' || p_sql);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'DEBUG: ' || p_sql);
        ELSE
            -- Execute the SQL statement with PARALLEL hint if applicable
            IF p_rollback THEN
                SAVEPOINT RB_POINT;
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Savepoint created for potential rollback.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'DEBUG: ' || 'Savepoint created for potential rollback.');
            END IF;
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' Executing SQL: ' || p_sql);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Executing SQL: ' || p_sql);
            EXECUTE IMMEDIATE p_sql;
            v_record_count := SQL%ROWCOUNT;
            IF p_rollback THEN
                ROLLBACK;
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Rollback deletion.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Rollback deletion.');
            END IF;
        END IF;
        RETURN v_record_count;
    END ExecuteSQL;

    PROCEDURE ProcessTable(p_table_name VARCHAR2, p_user_name VARCHAR2, p_action VARCHAR2, p_sqlstatement VARCHAR2, p_max_record_count NUMBER) IS
        v_matching_record_count NUMBER;
    BEGIN
        -- Log the STATS of table before Purge
        v_sql := 'SELECT num_rows, blocks*8/1024/1024 AS size_Gb ' ||
                 'FROM all_tables ' ||
                 'WHERE owner = UPPER(''' || p_user_name || ''') AND table_name = UPPER(''' || p_table_name || ''') ' ||
                 'ORDER BY 1, 2';

        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': run sql : ' || v_sql );
        -- Execute the query and log the results
        EXECUTE IMMEDIATE v_sql INTO v_record_count, v_table_size; -- This should remain executed regardless of debug mode
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': TABLE STATS BEFORE PURGE : ' || p_table_name || ' - Table Size :' || v_table_size || ' Record Count : ' || v_record_count);
        LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'TABLE STATS BEFORE PURGE : ' || p_table_name || ' - Table Size :' || v_table_size || ' Record Count : ' || v_record_count);

        -- Check if action is NULL or empty
        IF p_action IS NULL OR TRIM(p_action) = '' THEN
            RETURN; -- Skip to the next iteration if action is NULL or empty
        END IF;

        IF p_action = 'SQL' THEN
            v_sql := p_sqlstatement;
            -- Execute the SQL statement using the ExecuteSQL procedure
            v_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name);
        ELSIF p_action = 'CRITERIA' THEN
            -- Get the record count that matches the criteria
            v_sql := 'SELECT COUNT(*) /*+ PARALLEL(' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || ', 8) */ FROM ' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || p_sqlstatement;
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || v_sql);
            -- Execute the count query
            v_matching_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name);

            -- Check if matching record count exceeds the maximum
            IF v_matching_record_count > p_max_record_count THEN
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Record count (' || v_matching_record_count || ') exceeds maximum allowed (' || p_max_record_count || ') for table ' || p_table_name || '. Process aborted.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Record count (' || v_matching_record_count || ') exceeds maximum allowed (' || p_max_record_count || ') for table ' || p_table_name || '. Process aborted.');
                RETURN; -- Skip further processing for this table
            END IF;

            -- Disable foreign key constraints referencing the source_table
            BEGIN
                FOR constraint_rec IN (SELECT constraint_name, table_name
                                       FROM user_constraints
                                       WHERE r_constraint_name IN (SELECT constraint_name
                                                                   FROM user_constraints
                                                                   WHERE table_name = UPPER(p_table_name)
                                                                     AND constraint_type = 'P')
                                         AND constraint_type = 'R') -- Foreign key constraints
                    LOOP
                        v_constraint_name := constraint_rec.constraint_name;
                        v_sql := 'ALTER TABLE ' || constraint_rec.table_name || ' DISABLE CONSTRAINT ' || v_constraint_name;
                        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' - ' || 'Disabling constraint ' || v_constraint_name || '...');
                        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' - ' || ' Execute SQL Command for  ' || p_table_name || ': ' || v_sql);
                        EXECUTE IMMEDIATE v_sql;
                    END LOOP;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' - ' || 'Error during disable foreign key for table ' || v_table_name || ': ' || SQLERRM);
                    v_success_flag := FALSE; -- Set success flag to false
                    RETURN; -- Exit the loop if there's an error
            END;

            -- Now delete the records that match the criteria using parallel delete
            v_sql := 'DELETE /*+ PARALLEL(' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || ', 8) */ FROM ' || UPPER(p_user_name) || '.' || UPPER(p_table_name) || p_sqlstatement; -- Modified delete statement to use parallel
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Deleting records from ' || p_table_name);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Deleting records from ' || p_table_name);
--            b.comment
            v_record_count := ExecuteSQL(v_sql, p_user_name, p_table_name, v_toRollback);

            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Delete completed for ' || p_table_name || '. Total Records deleted: ' || v_record_count);
            LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Delete completed for ' || p_table_name || '. Total Records deleted: ' || v_record_count);

            -- Rollback if in debug mode
            IF v_toRollback THEN
                ROLLBACK; -- Rollback
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Rolled back to savepoint due to debug mode.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, 'Rolled back to savepoint due to debug mode.');
            ELSE
                commit;
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Commit deletions.');
                LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), p_user_name, p_table_name, ': Commit deletions.');
            END IF;
        END IF;
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': End of process table!');

    END ProcessTable;

BEGIN
    -- Initialize table records from the configuration table
    InitializeTables;

    -- Remove spaces from the parameter
    tables_to_process := REPLACE('&1', ' ', '');

    -- Check if the parameter is 'all' or empty
    IF TRIM(tables_to_process) IS NULL OR UPPER(tables_to_process) = 'ALL' THEN
        v_run_all := TRUE; -- Set flag to true to run for all tables
    ELSE
        -- Split the input parameter string into an array using commas
        DBMS_UTILITY.COMMA_TO_TABLE(tables_to_process, v_param_count, v_param_list);
    END IF;

    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Start Auto Purge with parameters: ' || tables_to_process || ' Parameters Number: ' || v_param_count);
    LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), NULL, NULL, 'Start Auto Purge with parameters: ' || tables_to_process || ' Parameters Number: ' || v_param_count);
    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Table count ' || tables.COUNT );

    -- Loop through each table in the array
    FOR i IN 1 .. tables.COUNT LOOP
            v_table_name := tables(i).table_name;
            v_user_name := tables(i).user_name;
            v_action := tables(i).action;
            v_sqlstatement := tables(i).sqlstatement;
            v_max_record_count := tables(i).max_record_count; -- Get max_record_count for the current table
            v_is_in_list := FALSE;
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Checking Parameters: ' || v_table_name || ' - ' || v_user_name || ' - ' || v_sqlstatement || ' - ' || v_max_record_count);

            -- If running all tables, set the flag to true
            IF v_run_all THEN
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': v_run_all is true ' || v_table_name || ' - ' || v_user_name || ' - ' || v_sqlstatement);
                v_is_in_list := TRUE;
            ELSE
                -- Check if the current table name exists in the parameter list
                FOR j IN 1 .. v_param_count LOOP
                        IF UPPER(v_table_name) = UPPER(v_param_list(j)) THEN
                            v_is_in_list := TRUE;
                            EXIT; -- Exit the loop once we find a match
                        END IF;
                    END LOOP;
            END IF;

            -- If running all tables or found in provided parameters
            IF v_is_in_list THEN
                -- Process the current table
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': start to process current table: ' || v_table_name || ' - ' || v_user_name || ' - ' || v_action || ' - ' || v_sqlstatement);
                ProcessTable(v_table_name, v_user_name, v_action, v_sqlstatement, v_max_record_count);
            END IF;
        END LOOP;

    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Completed for all tables!!! ');
    LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), NULL, NULL, 'Completed for all tables!!! ');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': Error occurred: ' || SQLERRM);
        LogToAudit(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'), NULL, NULL, 'Error occurred: ' || SQLERRM);
END;

