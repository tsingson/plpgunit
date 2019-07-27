/********************************************************************************
The PostgreSQL License

Copyright (c) 2014, Binod Nepal, Mix Open Foundation (http://mixof.org).

Permission to use, copy, modify, and distribute this software and its documentation 
for any purpose, without fee, and without a written agreement is hereby granted, 
provided that the above copyright notice and this paragraph and 
the following two paragraphs appear in all copies.

IN NO EVENT SHALL MIX OPEN FOUNDATION BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, 
SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, 
ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF 
MIX OPEN FOUNDATION HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MIX OPEN FOUNDATION SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, 
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, 
AND MIX OPEN FOUNDATION HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, 
UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
***********************************************************************************/
create schema if not exists assert;
create schema if not exists unit_tests;


set search_path = assert, unit_tests;
-- create domain public.test_result as text;
do $$
    begin
        if not EXISTS(select *
                      from
                          pg_type
                      where
                          typname = 'test_result' and
                              typnamespace = (
                              select
                                  oid
                              from
                                  pg_namespace
                              where
                                  nspname = 'unit_tests' ))
            then create domain unit_tests.test_result as text;
        end if;
    end
$$ language plpgsql;


drop table if exists unit_tests.test_details cascade;
drop table if exists unit_tests.tests cascade;
drop table if exists unit_tests.dependencies cascade;
create table unit_tests.tests
(
test_id serial not null primary key,
started_on timestamp without time zone not null default ( CURRENT_TIMESTAMP at time zone 'UTC' ),
completed_on timestamp without time zone null,
total_tests integer null default ( 0 ),
failed_tests integer null default ( 0 ),
skipped_tests integer null default ( 0 )
);

create index unit_tests_tests_started_on_inx on unit_tests.tests( started_on );

create index unit_tests_tests_completed_on_inx on unit_tests.tests( completed_on );

create index unit_tests_tests_failed_tests_inx on unit_tests.tests( failed_tests );

create table unit_tests.test_details
(
id bigserial not null primary key,
test_id integer not null references unit_tests.tests( test_id ),
function_name text not null,
message text not null,
ts timestamp without time zone not null default ( CURRENT_TIMESTAMP at time zone 'UTC' ),
status boolean not null,
executed boolean not null
);

create index unit_tests_test_details_test_id_inx on unit_tests.test_details( test_id );

create index unit_tests_test_details_status_inx on unit_tests.test_details( status );

create table unit_tests.dependencies
(
dependency_id bigserial not null primary key,
dependent_ns text,
dependent_function_name text not null,
depends_on_ns text,
depends_on_function_name text not null
);

create index unit_tests_dependencies_dependency_id_inx on unit_tests.dependencies( dependency_id );


drop function if exists assert.fail(message text
);
create function assert.fail
(message text
) returns text
as $$
begin
    if $1 is null or trim($1) = ''
        then
            message := 'NO REASON SPECIFIED';
    end if;
    raise warning 'ASSERT FAILED : %', message;
    return message;
end
$$ language plpgsql immutable
                    strict;

drop function if exists assert.pass(message text
);
create function assert.pass
(message text
) returns text
as $$
begin
    raise notice 'ASSERT PASSED : %', message;
    return '';
end
$$ language plpgsql immutable
                    strict;

drop function if exists assert.ok(message text
);
create function assert.ok
(message text
) returns text
as $$
begin
    raise notice 'OK : %', message;
    return '';
end
$$ language plpgsql immutable
                    strict;

drop function if exists assert.is_equal(in have anyelement,in want anyelement,out message text,out result boolean
);
create function assert.is_equal
(in have anyelement,
 in want anyelement,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 is not distinct from $2 )
        then
            message := 'Assert is equal.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_EQUAL FAILED.\n\nHave -> ' || COALESCE($1::text, 'NULL') || E'\nWant -> ' ||
               COALESCE($2::text, 'NULL') || E'\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;


drop function if exists assert.are_equal(variadic anyarray,out message text,out result boolean
);
create function assert.are_equal
(variadic anyarray,
 out message text,
 out result boolean
)
as $$
declare
    count integer=0; declare
    total_items bigint; declare
    total_rows bigint;
begin
    result := false;
    with
        counter as (
            select *
            from
                explode_array($1) as items )
    select
        COUNT(items),
        COUNT(*)
    into total_items, total_rows
    from
        counter;
    if ( total_items = 0 or total_items = total_rows )
        then
            result := true;
    end if;
    if ( result and total_items > 0 )
        then
            select
                COUNT(distinct $1[s.i])
            into count
            from
                generate_series(array_lower($1, 1), array_upper($1, 1)) as s( i )
            order by 1;
            if count <> 1
                then
                    result := false;
            end if;
    end if;
    if ( not result )
        then
            message := 'ASSERT ARE_EQUAL FAILED.';
            perform assert.fail(message);
            return;
    end if;
    message := 'Asserts are equal.';
    perform assert.ok(message);
    result := true;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.is_not_equal(in already_have anyelement,in dont_want anyelement,out message text,out result boolean
);
create function assert.is_not_equal
(in already_have anyelement,
 in dont_want anyelement,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 is distinct from $2 )
        then
            message := 'Assert is not equal.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_NOT_EQUAL FAILED.\n\nAlready Have -> ' || COALESCE($1::text, 'NULL') ||
               E'\nDon''t Want   -> ' || COALESCE($2::text, 'NULL') || E'\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.are_not_equal(variadic anyarray,out message text,out result boolean
);
create function assert.are_not_equal
(variadic anyarray,
 out message text,
 out result boolean
)
as $$
declare
    count integer=0; declare
    count_nulls bigint;
begin
    select
        COUNT(*)
    into count_nulls
    from
        explode_array($1) as items
    where
        items is null;
    select
        COUNT(distinct $1[s.i])
    into count
    from
        generate_series(array_lower($1, 1), array_upper($1, 1)) as s( i )
    order by 1;
    if ( count + count_nulls <> array_upper($1, 1) or count_nulls > 1 )
        then
            message := 'ASSERT ARE_NOT_EQUAL FAILED.';
            perform assert.fail(message);
            result := false;
            return;
    end if;
    message := 'Asserts are not equal.';
    perform assert.ok(message);
    result := true;
    return;
end
$$ language plpgsql immutable;


drop function if exists assert.is_null(in anyelement,out message text,out result boolean
);
create function assert.is_null
(in anyelement,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 is null )
        then
            message := 'Assert is NULL.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_NULL FAILED. NULL value was expected.\n\n\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.is_not_null(in anyelement,out message text,out result boolean
);
create function assert.is_not_null
(in anyelement,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 is not null )
        then
            message := 'Assert is not NULL.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_NOT_NULL FAILED. The value is NULL.\n\n\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.is_true(in boolean,out message text,out result boolean
);
create function assert.is_true
(in boolean,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 )
        then
            message := 'Assert is true.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_TRUE FAILED. A true condition was expected.\n\n\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.is_false(in boolean,out message text,out result boolean
);
create function assert.is_false
(in boolean,
 out message text,
 out result boolean
)
as $$
begin
    if ( not $1 )
        then
            message := 'Assert is false.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_FALSE FAILED. A false condition was expected.\n\n\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.is_greater_than(in x anyelement,in y anyelement,out message text,out result boolean
);
create function assert.is_greater_than
(in x anyelement,
 in y anyelement,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 > $2 )
        then
            message := 'Assert greater than condition is satisfied.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_GREATER_THAN FAILED.\n\n X : -> ' || COALESCE($1::text, 'NULL') ||
               E'\n is not greater than Y:   -> ' || COALESCE($2::text, 'NULL') || E'\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.is_less_than(in x anyelement,in y anyelement,out message text,out result boolean
);
create function assert.is_less_than
(in x anyelement,
 in y anyelement,
 out message text,
 out result boolean
)
as $$
begin
    if ( $1 < $2 )
        then
            message := 'Assert less than condition is satisfied.';
            perform assert.ok(message);
            result := true;
            return;
    end if;
    message := E'ASSERT IS_LESS_THAN FAILED.\n\n X : -> ' || COALESCE($1::text, 'NULL') ||
               E'\n is not less than Y:   -> ' || COALESCE($2::text, 'NULL') || E'\n';
    perform assert.fail(message);
    result := false;
    return;
end
$$ language plpgsql immutable;

drop function if exists assert.function_exists(function_name text,out message text,out result boolean
);
create function assert.function_exists
(function_name text,
 out message text,
 out result boolean
)
as $$
begin
    if not EXISTS(select
                      1
                  from
                      pg_catalog.pg_namespace n
                      join pg_catalog.pg_proc p
                           on pronamespace = n.oid
                  where
                          replace(nspname || '.' || proname || '(' || oidvectortypes(proargtypes) || ')', ' ',
                                  '')::text = $1)
        then
            message := format('The function %s does not exist.', $1);
            perform assert.fail(message);
            result := false;
            return;
    end if;
    message := format('Ok. The function %s exists.', $1);
    perform assert.ok(message);
    result := true;
    return;
end
$$ language plpgsql;

drop function if exists assert.if_functions_compile(variadic _schema_name text[],out message text,out result boolean
);
create or replace function assert.if_functions_compile
(variadic _schema_name text[],
 out message text,
 out result boolean
)
as $$
declare
    all_parameters text; declare
    current_function record; declare
    current_function_name text; declare
    current_type text; declare
    current_type_schema text; declare
    current_parameter text; declare
    functions_count smallint := 0; declare
    current_parameters_count int; declare
    i int; declare
    command_text text; declare
    failed_functions text;
begin
    for current_function in select
                                proname,
                                proargtypes,
                                nspname
                            from
                                pg_proc
                                inner join pg_namespace
                                           on pg_proc.pronamespace = pg_namespace.oid
                            where
                                    pronamespace in (
                                    select
                                        oid
                                    from
                                        pg_namespace
                                    where
                                        nspname = any ( $1 ) and
                                        nspname not in ( 'assert','unit_tests','information_schema' ) and
                                        proname not in ( 'if_functions_compile' ) )
        loop
            current_parameters_count := array_upper(current_function.proargtypes, 1) + 1;
            i := 0;
            all_parameters := '';
            loop
                if i < current_parameters_count
                    then
                        if i > 0
                            then
                                all_parameters := all_parameters || ', ';
                        end if;
                        select
                            nspname,
                            typname
                        into current_type_schema, current_type
                        from
                            pg_type
                            inner join pg_namespace
                                       on pg_type.typnamespace = pg_namespace.oid
                        where
                            pg_type.oid = current_function.proargtypes[i];
                        if ( current_type in ( 'int4','int8','numeric','integer_strict','money_strict','decimal_strict',
                                               'integer_strict2','money_strict2','decimal_strict2','money','decimal',
                                               'numeric','bigint' ) )
                            then
                                current_parameter := '1::' || current_type_schema || '.' || current_type;
                            elsif ( substring(current_type, 1, 1) = '_' )
                                then
                                    current_parameter := 'NULL::' || current_type_schema || '.' ||
                                                         substring(current_type, 2, length(current_type)) || '[]';
                            elsif ( current_type in ( 'date' ) )
                                then
                                    current_parameter := '''1-1-2000''::' || current_type;
                            elsif ( current_type = 'bool' )
                                then
                                    current_parameter := 'false';
                            else
                                current_parameter := '''''::' || quote_ident(current_type_schema) || '.' ||
                                                     quote_ident(current_type);
                        end if;
                        all_parameters = all_parameters || current_parameter;
                        i := i + 1;
                    else
                        exit;
                end if;
            end loop;
            begin
                current_function_name :=
                            quote_ident(current_function.nspname) || '.' || quote_ident(current_function.proname);
                command_text := 'SELECT * FROM ' || current_function_name || '(' || all_parameters || ');';
                execute command_text;
                functions_count := functions_count + 1;
            exception
                when others then if ( failed_functions is null )
                    then
                        failed_functions := '';
                end if;
                if ( sqlstate in ( '42702','42704' ) )
                    then
                        failed_functions := failed_functions || E'\n' || command_text || E'\n' || sqlerrm || E'\n';
                end if;
            end;
        end loop;
    if ( failed_functions != '' )
        then
            message := E'The test if_functions_compile failed. The following functions failed to compile : \n\n' ||
                       failed_functions;
            result := false;
            perform assert.fail(message);
            return;
    end if;
end;
$$ language plpgsql volatile;

drop function if exists assert.if_views_compile(variadic _schema_name text[],out message text,out result boolean
);
create function assert.if_views_compile
(variadic _schema_name text[],
 out message text,
 out result boolean
)
as $$
declare
    message unit_tests.test_result; declare
    current_view record; declare
    current_view_name text; declare
    command_text text; declare
    failed_views text;
begin
    for current_view in select
                            table_name,
                            table_schema
                        from
                            information_schema.views
                        where
                            table_schema = any ( $1 )
        loop
            begin
                current_view_name :=
                            quote_ident(current_view.table_schema) || '.' || quote_ident(current_view.table_name);
                command_text := 'SELECT * FROM ' || current_view_name || ' LIMIT 1;';
                raise notice '%', command_text;
                execute command_text;
            exception
                when others then if ( failed_views is null )
                    then
                        failed_views := '';
                end if;
                failed_views := failed_views || E'\n' || command_text || E'\n' || sqlerrm || E'\n';
            end;
        end loop;
    if ( failed_views != '' )
        then
            message := E'The test if_views_compile failed. The following views failed to compile : \n\n' ||
                       failed_views;
            result := false;
            perform assert.fail(message);
            return;
    end if;
    return;
end;
$$ language plpgsql volatile;


drop function if exists unit_tests.add_dependency(p_dependent text,p_depends_on text
);
create function unit_tests.add_dependency
(p_dependent text,
 p_depends_on text
) returns void
as $$
declare
    dependent_ns text; declare
    dependent_name text; declare
    depends_on_ns text; declare
    depends_on_name text; declare
    arr text[];
begin
    if p_dependent like '%.%'
        then
            select
                regexp_split_to_array(p_dependent, E'\\.')
            into arr;
            select
                arr[1]
            into dependent_ns;
            select
                arr[2]
            into dependent_name;
        else
            select
                null
            into dependent_ns;
            select
                p_dependent
            into dependent_name;
    end if;
    if p_depends_on like '%.%'
        then
            select
                regexp_split_to_array(p_depends_on, E'\\.')
            into arr;
            select
                arr[1]
            into depends_on_ns;
            select
                arr[2]
            into depends_on_name;
        else
            select
                null
            into depends_on_ns;
            select
                p_depends_on
            into depends_on_name;
    end if;
    insert
    into
        unit_tests.dependencies
    (dependent_ns,dependent_function_name,depends_on_ns,depends_on_function_name)
    values
    (dependent_ns,dependent_name,depends_on_ns,depends_on_name);
end
$$ language plpgsql strict;


drop function if exists unit_tests.begin(verbosity integer,format text
);
create function unit_tests.begin
(verbosity integer default 9,
 format text default ''
)
    returns table
            (
            message text,
            result character(1)
            )
as $$
declare
    this record; declare
    _function_name text; declare
    _sql text; declare
    _failed_dependencies text[]; declare
    _num_of_test_functions integer; declare
    _should_skip boolean; declare
    _message text; declare
    _error text; declare
    _context text; declare
    _result character(1); declare
    _test_id integer; declare
    _status boolean; declare
    _total_tests integer = 0; declare
    _failed_tests integer = 0; declare
    _skipped_tests integer = 0; declare
    _list_of_failed_tests text; declare
    _list_of_skipped_tests text; declare
    _started_from timestamp without time zone; declare
    _completed_on timestamp without time zone; declare
    _delta integer; declare
    _ret_val text = ''; declare
    _verbosity text[] = array ['debug5', 'debug4', 'debug3', 'debug2', 'debug1', 'log', 'notice', 'warning', 'error', 'fatal', 'panic'];
begin
    _started_from := clock_timestamp() at time zone 'UTC';
    if ( format = 'teamcity' )
        then
            raise info '##teamcity[testSuiteStarted name=''Plpgunit'' message=''Test started from : %'']', _started_from;
        else
            raise info 'Test started from : %', _started_from;
    end if;
    if ( $1 > 11 )
        then
            $1 := 9;
    end if;
    execute 'SET CLIENT_MIN_MESSAGES TO ' || _verbosity[$1];
    raise warning 'CLIENT_MIN_MESSAGES set to : %' , _verbosity[$1];
    select
        nextval('unit_tests.tests_test_id_seq')
    into _test_id;
    insert
    into
        unit_tests.tests(test_id)
    select
        _test_id;
    drop table if exists temp_test_functions;
    create temp table temp_test_functions
    as
        select
            nspname as ns_name,
            proname as function_name,
            p.oid as oid
        from
            pg_catalog.pg_namespace n
            join pg_catalog.pg_proc p
                 on pronamespace = n.oid
        where
            prorettype = 'test_result'::regtype::oid;
    select
        count(*)
    into _num_of_test_functions
    from
        temp_test_functions;
    drop table if exists temp_dependency_levels;
    create temp table temp_dependency_levels
    as
        with recursive
            dependency_levels( ns_name,function_name,oid,level ) as (
                -- select functions without any dependencies
                select
                    ns_name,
                    function_name,
                    tf.oid,
                    0 as level
                from
                    temp_test_functions tf
                    left outer join unit_tests.dependencies d
                                    on tf.ns_name = d.dependent_ns and tf.function_name = d.dependent_function_name
                where
                    d.dependency_id is null
                union
                -- add functions which depend on the previous level functions
                select
                    d.dependent_ns,
                    d.dependent_function_name,
                    tf.oid,
                    level + 1
                from
                    dependency_levels dl
                    join unit_tests.dependencies d
                         on dl.ns_name = d.depends_on_ns and dl.function_name like d.depends_on_function_name
                    join temp_test_functions tf
                         on d.dependent_ns = tf.ns_name and d.dependent_function_name = tf.function_name
                where
                    level < _num_of_test_functions -- don't follow circles for too long
            )
        select
            ns_name,
            function_name,
            oid,
            max(level) as max_level
        from
            dependency_levels
        group by ns_name, function_name, oid;
    if (
        select
            count(*) < _num_of_test_functions
        from
            temp_dependency_levels )
        then
            select
                array_to_string(array_agg(tf.ns_name || '.' || tf.function_name || '()'), ', ')
            into _error
            from
                temp_test_functions tf
                left outer join temp_dependency_levels dl
                                on tf.oid = dl.oid
            where
                dl.oid is null;
            raise exception 'Cyclic dependencies detected. Check the following test functions: %', _error;
    end if;
    if exists(select *
              from
                  temp_dependency_levels
              where
                  max_level = _num_of_test_functions)
        then
            select
                array_to_string(array_agg(ns_name || '.' || function_name || '()'), ', ')
            into _error
            from
                temp_dependency_levels
            where
                max_level = _num_of_test_functions;
            raise exception 'Cyclic dependencies detected. Check the dependency graph including following test functions: %', _error;
    end if;
    for this in select
                    ns_name,
                    function_name,
                    max_level
                from
                    temp_dependency_levels
                order by max_level, oid
        loop
            begin
                _status := false;
                _total_tests := _total_tests + 1;
                _function_name = this.ns_name || '.' || this.function_name || '()';
                select
                    array_agg(td.function_name)
                into _failed_dependencies
                from
                    unit_tests.dependencies d
                    join unit_tests.test_details td
                         on td.function_name like d.depends_on_ns || '.' || d.depends_on_function_name || '()'
                where
                    d.dependent_ns = this.ns_name and
                    d.dependent_function_name = this.function_name and
                    test_id = _test_id and
                    status = false;
                select
                    _failed_dependencies is not null
                into _should_skip;
                if not _should_skip
                    then
                        _sql := 'SELECT ' || _function_name || ';';
                        raise notice 'RUNNING TEST : %.', _function_name;
                        if ( format = 'teamcity' )
                            then
                                raise info '##teamcity[testStarted name=''%'' message=''%'']', _function_name, _started_from;
                            else
                                raise info 'Running test % : %', _function_name, _started_from;
                        end if;
                        execute _sql into _message;
                        if _message = ''
                            then
                                _status := true;
                                if ( format = 'teamcity' )
                                    then
                                        raise info '##teamcity[testFinished name=''%'' message=''%'']', _function_name, clock_timestamp() at time zone 'UTC';
                                    else
                                        raise info 'Passed % : %', _function_name, clock_timestamp() at time zone 'UTC';
                                end if;
                            else
                                if ( format = 'teamcity' )
                                    then
                                        raise info '##teamcity[testFailed name=''%'' message=''%'']', _function_name, _message;
                                        raise info '##teamcity[testFinished name=''%'' message=''%'']', _function_name, clock_timestamp() at time zone 'UTC';
                                    else
                                        raise info 'Test failed % : %', _function_name, _message;
                                end if;
                        end if;
                    else
                        -- skipped test
                        _status := true;
                        _message = 'Failed dependencies: ' || array_to_string(_failed_dependencies, ',');
                        if ( format = 'teamcity' )
                            then
                                raise info '##teamcity[testSkipped name=''%''] : %', _function_name, clock_timestamp() at time zone 'UTC';
                            else
                                raise info 'Skipped % : %', _function_name, clock_timestamp() at time zone 'UTC';
                        end if;
                end if;
                insert
                into
                    unit_tests.test_details(test_id,function_name,message,status,executed,ts)
                select
                    _test_id,
                    _function_name,
                    _message,
                    _status,
                    not _should_skip,
                    clock_timestamp();
                if not _status
                    then
                        _failed_tests := _failed_tests + 1;
                        raise warning 'TEST % FAILED.', _function_name;
                        raise warning 'REASON: %', _message;
                    elsif not _should_skip
                        then
                            raise notice 'TEST % COMPLETED WITHOUT ERRORS.', _function_name;
                    else
                        _skipped_tests := _skipped_tests + 1;
                        raise warning 'TEST % SKIPPED, BECAUSE A DEPENDENCY FAILED.', _function_name;
                end if;
            exception
                when others then get stacked diagnostics _context = pg_exception_context;
                _message := 'ERR: [' || sqlstate || ']: ' || sqlerrm || E'\n    ' || split_part(_context, E'\n', 1);
                insert
                into
                    unit_tests.test_details(test_id,function_name,message,status,executed)
                select
                    _test_id,
                    _function_name,
                    _message,
                    false,
                    true;
                _failed_tests := _failed_tests + 1;
                raise warning 'TEST % FAILED.', _function_name;
                raise warning 'REASON: %', _message;
                if ( format = 'teamcity' )
                    then
                        raise info '##teamcity[testFailed name=''%'' message=''%'']', _function_name, _message;
                        raise info '##teamcity[testFinished name=''%'' message=''%'']', _function_name, clock_timestamp() at time zone 'UTC';
                    else
                        raise info 'Test failed % : %', _function_name, _message;
                end if;
            end;
        end loop;
    _completed_on := clock_timestamp() at time zone 'UTC';
    _delta := extract(millisecond from _completed_on - _started_from)::integer;
    update unit_tests.tests
    set
        total_tests = _total_tests,
        failed_tests = _failed_tests,
        skipped_tests = _skipped_tests,
        completed_on = _completed_on
    where
        test_id = _test_id;
    if format = 'junit'
        then
            select
                    '<?xml version="1.0" encoding="UTF-8"?>' || xmlelement(name testsuites,
                                                                           xmlelement(name testsuite, xmlattributes(
                                                                                      'plpgunit' as name,
                                                                                      t.total_tests as tests,
                                                                                      t.failed_tests as failures, 0 as
                                                                                      errors,
                                                                                      EXTRACT(epoch from t.completed_on - t.started_on) as
                                                                                      time),
                                                                                      xmlagg(xmlelement(name testcase,
                                                                                                        xmlattributes(
                                                                                                        td.function_name as
                                                                                                        name,
                                                                                                        EXTRACT(epoch from td.ts - t.started_on) as
                                                                                                        time), case
                                                                                                                   when td.status = false
                                                                                                                       then
                                                                                                                       xmlelement(name failure, td.message)
                                                                                                               end))))
            into _ret_val
            from
                unit_tests.test_details td,
                unit_tests.tests t
            where
                t.test_id = _test_id and
                td.test_id = t.test_id
            group by t.test_id;
        else
            with
                failed_tests as (
                    select
                        row_number() over (order by id) as id,
                        unit_tests.test_details.function_name,
                        unit_tests.test_details.message
                    from
                        unit_tests.test_details
                    where
                        test_id = _test_id and
                        status = false )
            select
                array_to_string(array_agg(f.id::text || '. ' || f.function_name || ' --> ' || f.message), E'\n')
            into _list_of_failed_tests
            from
                failed_tests f;
            with
                skipped_tests as (
                    select
                        row_number() over (order by id) as id,
                        unit_tests.test_details.function_name,
                        unit_tests.test_details.message
                    from
                        unit_tests.test_details
                    where
                        test_id = _test_id and
                        executed = false )
            select
                array_to_string(array_agg(s.id::text || '. ' || s.function_name || ' --> ' || s.message), E'\n')
            into _list_of_skipped_tests
            from
                skipped_tests s;
            _ret_val := _ret_val || 'Test completed on : ' || _completed_on::text || E' UTC. \nTotal test runtime: ' ||
                        _delta::text || E' ms.\n';
            _ret_val := _ret_val || E'\nTotal tests run : ' || COALESCE(_total_tests, '0')::text;
            _ret_val := _ret_val || E'.\nPassed tests    : ' ||
                        ( COALESCE(_total_tests, '0') - COALESCE(_failed_tests, '0') -
                          COALESCE(_skipped_tests, '0') )::text;
            _ret_val := _ret_val || E'.\nFailed tests    : ' || COALESCE(_failed_tests, '0')::text;
            _ret_val := _ret_val || E'.\nSkipped tests   : ' || COALESCE(_skipped_tests, '0')::text;
            _ret_val := _ret_val || E'.\n\nList of failed tests:\n' || '----------------------';
            _ret_val := _ret_val || E'\n' || COALESCE(_list_of_failed_tests, '<NULL>')::text;
            _ret_val := _ret_val || E'.\n\nList of skipped tests:\n' || '----------------------';
            _ret_val := _ret_val || E'\n' || COALESCE(_list_of_skipped_tests, '<NULL>')::text;
            _ret_val := _ret_val || E'\n' || E'End of plpgunit test.\n\n';
    end if;
    if _failed_tests > 0
        then
            _result := 'N';
            if ( format = 'teamcity' )
                then
                    raise info '##teamcity[testStarted name=''Result'']';
                    raise info '##teamcity[testFailed name=''Result'' message=''%'']', REPLACE(_ret_val, E'\n', ' |n');
                    raise info '##teamcity[testFinished name=''Result'']';
                    raise info '##teamcity[testSuiteFinished name=''Plpgunit'' message=''%'']', REPLACE(_ret_val, E'\n', '|n');
                else
                    raise info '%', _ret_val;
            end if;
        else
            _result := 'Y';
            if ( format = 'teamcity' )
                then
                    raise info '##teamcity[testSuiteFinished name=''Plpgunit'' message=''%'']', REPLACE(_ret_val, E'\n', '|n');
                else
                    raise info '%', _ret_val;
            end if;
    end if;
    set CLIENT_MIN_MESSAGES to notice;
    return query select _ret_val, _result;
end
$$ language plpgsql;

drop function if exists unit_tests.begin_junit(verbosity integer
);
create function unit_tests.begin_junit
(verbosity integer default 9
)
    returns table
            (
            message text,
            result character(1)
            )
as $$
begin
    return query select *
                 from
                     unit_tests.begin($1, 'junit');
end
$$ language plpgsql;

-- version of begin that will raise if any tests have failed
-- this will cause psql to return nonzeo exit code so the build/script can be halted
create or replace function unit_tests.begin_psql
(verbosity integer default 9,
 format text default ''
) returns void
as $$
declare
    _msg text;
    _res character(1);
begin
    select *
    into _msg, _res
    from
        unit_tests.begin(verbosity, format);
    if ( _res != 'Y' )
        then
            raise exception 'Tests failed [%]', _msg;
    end if;
end
$$ language plpgsql;

