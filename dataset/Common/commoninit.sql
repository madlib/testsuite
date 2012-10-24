create or replace function madlibtestdata.platform_chck(tname text) returns void as $$
declare
  cnt int;
begin
  select count(*) from pg_attribute where attrelid = tname::regclass::oid
    into cnt;
end;
$$ language plpgsql;

create or replace function madlibtestdata.platform_train(tname text) returns text as $$
declare
begin
  perform madlibtestdata.platform_chck(tname);
  return 'OK';
end;
$$ language plpgsql;

create or replace function madlibtestdata.platform_test(tname text) returns text as $$
declare
  result text;
begin
  result = madlibtestdata.platform_train(tname);
  return result;
end;
$$ language plpgsql;
