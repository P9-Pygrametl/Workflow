begin;

-- schema
drop schema if exists pygrametlexa cascade;
create schema pygrametlexa;
set search_path to pygrametlexa;


-- tables
create table page(
   pageid int,
   url varchar,
   size int,
   domain varchar,
   topleveldomain varchar,
   serverversion varchar,
   server varchar,
   validfrom date,
   validto date,
   version int
);

create table test(
   testid int,
   testname varchar,
   testauthor varchar
);
insert into test values (0, 'Test0', 'Joe Doe'), (1, 'Test1', 'Joe Doe'), 
(2, 'Test2', 'Joe Doe'), (3, 'Test3', 'Joe Doe'), (4, 'Test4', 'Joe Doe'),
(-1, 'Unknown test', 'Unknown author');


create table date(
   dateid int,
   date date,
   day int,
   month int,
   year int,
   week int,
   weekyear int
);

create table testresults(
   pageid int,
   testid int,
   dateid int,
   errors int
);


-- primary keys
alter table page add primary key(pageid);
alter table test add primary key(testid);
alter table date add primary key(dateid);
alter table testresults add primary key(pageid, testid, dateid);


-- foreign keys
-- alter table testresults add foreign key(pageid) references page,
--   add foreign key(testid) references test,
--   add foreign key(dateid) references date;


-- indexes
create index url_version_idx on page(url, version desc);

commit;