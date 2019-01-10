-----------------------------------------------------------------------------------------------------------------------------------------------
---- textfile格式，字段包含 关联
-----------------------------------------------------------------------------------------------------------------------------------------------
drop table tmp.test_t1;
create table tmp.test_t1 (
  b string,
  m string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as textFile
;
--hdfs://nameservice1/user/hive/warehouse/tmp.db/test_t1


drop table tmp.test_t2;
create table tmp.test_t2 (
  bm string,
  b string,
  m string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as textFile
;
--hdfs://nameservice1/user/hive/warehouse/tmp.db/test_t2



-- 写文件------------
test1:
b1,m1
b2,m2
b3,m3

test2:

bm1,bb1,m1
bm1,bb1,m2
bm2,bb2,m3

-- 载入文件------------
load data LOCAL INPATH './test1'  overwrite into table tmp.test_t1;
load data LOCAL INPATH './test2'  overwrite into table tmp.test_t2;

-- 查询确认-----------
select * from tmp.test_t1 t1 ,tmp.test_t2 t2 where t1.b= t2.b;
select * from tmp.test_t1 t1 ,tmp.test_t2 t2 where t1.m= t2.m;
select * from  tmp.test_t1  t1,tmp.test_t2 t2 where  t2.b like '%t1.b%';
select * from  tmp.test_t1  t1,tmp.test_t2 t2 where  t2.b like concat('%',t1.b,'%');



-----------------------------------------------------------------------------------------------------------------------------------------------
---- parquet格式测试
-----------------------------------------------------------------------------------------------------------------------------------------------
drop table tmp.test_t1;
create table tmp.test_t1 (
  b string,
  m string
)
stored as parquet
;

--hadoop fs -ls hdfs://nameservice1/user/hive/warehouse/tmp.db/test_t1
--不要加分号
select * from tmp.test_t1;

insert into tmp.test_t1 values('bb1','mm1');
insert into tmp.test_t1 values('bb2','mm2');
insert into tmp.test_t1 values('bb3','mm3');
insert into tmp.test_t1 values('bb4','mm4');
insert into tmp.test_t1 values('bb5','mm5');
insert into tmp.test_t1 values('bb6','mm6');


