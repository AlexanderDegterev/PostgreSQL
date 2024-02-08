CREATE OR REPLACE PROCEDURE admin.creating_partitions_one_day(
    p_parent_table text -- родительская таблица
, p_start_partition date -- дата начала партицирования
, p_stop_partition date -- дата окончания партицирования
, p_tablespace text -- указание названия табличного пространства
, p_primary_key text -- название поля, по которому будет создан PRIMARY KEY
, p_owner text -- указываем владельца таблицы
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_partattrs            smallint[];
    v_higher_parent_schema text := split_part(p_parent_table, '.', 1);
    v_higher_parent_table  text := split_part(p_parent_table, '.', 2);
    v_parent_schema        text;
    v_parent_tablename     text;
    v_partstrat            char;
    v_one_date_later       date;
    v_part_name            text;
    v_pkey_name            text;
    v_unlogged             char;
    d_time                 record;
    v_tablespace           text;
    v_query                text;

BEGIN
    /*
     creating_partitions_one_day
     * Function to turn a table into the parent of a partition set
     CALL admin.creating_partitions_one_day('admin.test_table','2024-01-01', '2024-01-02', 'pg_default', 'cdr_id', 'lbis');
     */

/*  Проверка версии PostgreSQL +   */
    IF current_setting('server_version_num')::int < 140000 THEN
        RAISE EXCEPTION 'PARTITION requires PostgreSQL 14 or greater';
    END IF;

/*  Проверка корректности схема+таблица+   */
    IF array_length(string_to_array(p_parent_table, '.'), 1) < 2 THEN
        RAISE EXCEPTION 'Parent table must be schema qualified';
    ELSIF array_length(string_to_array(p_parent_table, '.'), 1) > 2 THEN
        RAISE EXCEPTION 'This procedure does not support objects with periods in their names';
    END IF;

/*  Проверка существования табличного пространства   */
    SELECT spcname INTO v_tablespace FROM pg_tablespace WHERE spcname = p_tablespace;
    IF v_tablespace IS NULL THEN
        RAISE EXCEPTION 'This tablespace: % not found, please create tablespace', p_tablespace;
    END IF;

-- Check if given parent table has been already set up as a partitioned table and is ranged
    SELECT p.partstrat, partattrs
    INTO v_partstrat, v_partattrs
    FROM pg_catalog.pg_partitioned_table p
             JOIN pg_catalog.pg_class c ON p.partrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = v_parent_schema::name
      AND c.relname = v_parent_tablename::name;
    --Стратегия секционирования; h = секционирование по хешу (Hash),
-- l = секционирование по спискам (List),
-- r = секционирование по диапазонам (Range)
    IF v_partstrat <> 'r' /*OR v_partstrat IS NULL*/ THEN
        RAISE EXCEPTION 'You must have created the given parent table as ranged (not list) partitioned already. Ex: CREATE TABLE ... PARTITION BY RANGE ...)';
    END IF;
    /*  Проверка на кол-во полей учавствующих в партицировании  */
    IF array_length(v_partattrs, 1) > 1 THEN
        RAISE NOTICE 'This procedure only supports single column partitioning at this time. Found % columns in given parent definition.', array_length(v_partattrs, 1);
    END IF;

-- Check parent table
    SELECT n.nspname, c.relname, c.relpersistence
    INTO v_parent_schema, v_parent_tablename, v_unlogged
    FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = split_part(p_parent_table, '.', 1)::name
      AND c.relname = split_part(p_parent_table, '.', 2)::name;
    IF v_parent_tablename IS NULL THEN
        RAISE EXCEPTION 'Unable to find given parent table in system catalogs. Please create parent table first: %', p_parent_table;
    END IF;

    /* Запускаем цикл по датам*/
    FOR d_time IN SELECT aa :: DATE
                  FROM generate_series(p_start_partition, p_stop_partition, interval '1 day') s (aa)
        LOOP
            v_query := '';
            -- ??? EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', v_parent_schema, v_parent_tablename);
            v_one_date_later := d_time.aa + 1;
            v_part_name := to_char(d_time.aa, 'yyyymmdd');
            v_part_name := p_parent_table || '_p' || v_part_name;
            v_pkey_name := v_part_name || '_pkey';
            --CREATE TABLE lbis.cdr_p20240201 PARTITION OF lbis.cdr FOR VALUES FROM ('2024-02-01') TO ('2024-02-02') TABLESPACE lbis_cdr_2024;
            --raise notice 'CREATE TABLE % PARTITION OF % FROM (''%'') TO (''%'') TABLESPACE %;', v_part_name,p_parent_table,d_time.aa, v_one_date_later,p_tablespace;
            v_query := 'CREATE TABLE ' || v_part_name || ' PARTITION OF ' || p_parent_table || ' FOR VALUES FROM (''' ||
                       d_time.aa || ''') TO (''' || v_one_date_later || ''') TABLESPACE ' || p_tablespace || ';';
            raise notice '%', v_query;
            EXECUTE v_query;
            -- CREATE UNIQUE INDEX CONCURRENTLY items_pk ON items (id) TABLESPACE indexspace;; -- занимает много времени, но не блокирует запросы
            -- test_table_p20240101_pkey
            -- ALTER TABLE items ADD CONSTRAINT items_pk PRIMARY KEY USING INDEX items_pk;  -- блокирует запросы, но ненадолго

            -- использовать CONCURRENTLY не получилось по причине :
            -- Такие операторы, как CREATE INDEX CONCURRENTLY, которые нельзя выполнить в явной транзакции (то есть внутри пары BEGIN/COMMIT), также нельзя запустить в неявной транзакции.

            IF char_length(p_primary_key) > 0 THEN
                v_query := ' CREATE UNIQUE INDEX ' || split_part(v_pkey_name, '.', 2)::name || ' ON ' || v_part_name ||
                           ' (' || p_primary_key || ') TABLESPACE ' || p_tablespace || ';';
                raise notice '%', v_query;
                EXECUTE v_query;
                v_query := 'ALTER TABLE ' || v_part_name || ' ADD CONSTRAINT ' ||
                           split_part(v_pkey_name, '.', 2)::name || ' PRIMARY KEY USING INDEX ' ||
                           split_part(v_pkey_name, '.', 2)::name || ';';
                raise notice '%', v_query;
                EXECUTE v_query;

                v_query := ' ALTER TABLE ' || v_part_name || ' SET TABLESPACE ' || p_tablespace || ';';
                raise notice '%', v_query;
                EXECUTE v_query;
            END IF;

            IF char_length(p_owner) > 0 THEN
                v_query := ' ALTER TABLE ' || v_part_name || ' owner to ' || p_owner || ';';
                raise notice '%', v_query;
                EXECUTE v_query;
            END IF;
            v_one_date_later := v_one_date_later + 1;
            -- INDEX
            -- CREATE INDEX CONCURRENTLY
            -- PRIMARY KEY
            -- CREATE UNIQUE INDEX CONCURRENTLY items_pk ON items (id); -- занимает много времени, но не блокирует запросы
            -- ALTER TABLE items ADD CONSTRAINT items_pk PRIMARY KEY USING INDEX items_pk;  -- блокирует запросы, но ненадолго
            COMMIT;
        END LOOP;

END

$$;