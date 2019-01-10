
-------------------------------------------------------------- 基本使用
#打印列的头
show functions;
set hive.cli.print.header=true;

#数组
1. array拆成多列
SELECT explode(b) AS bx FROM test ;

2. 和其他列一起输出
SELECT id, tag FROM test LATERAL VIEW explode(b) tmpxx AS tag;
3. insert into test VALUES  (3,array('c','b'));

#case when
count(case when p.imei is null then 0 when p.imei='sss' then 2 else 1 end )

# 字段a包含字段b,关联
select * from  tmp.test_t1  t1,tmp.test_t2 t2 where  t2.b like concat('%',t1.b,'%')

#数据文件删除后，hive同步更新元数据(删除partition)
#https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RecoverPartitions(MSCKREPAIRTABLE)

msck repair table ssp.ods_f_request [SYNC PARTITIONS];
use ssp; msck repair table ods_f_request SYNC PARTITIONS;
MSCK REPAIR TABLE ssp.ods_f_request DROP PARTITIONS
MSCK [REPAIR] TABLE table_name [ADD/DROP/SYNC PARTITIONS];

---------------------------------------------------------------------------------------
----------[group by null的做法](https://academy.vertabelo.com/blog/null-values-group-clause/)


hive里面group by不出来null值
SELECT department, count(*)
FROM employee
GROUP BY department;

msyql可以
SELECT coalesce(department,’Unassigned department’), count(*)
FROM employee
GROUP BY 1;

---------------------------------------------------------------------------------------
----------------------[4.插入array](https://community.hortonworks.com/questions/22262/insert-values-in-array-data-type-hive.html)
insert into ad_flow PARTITION (data_date = 2018062513)
  select 'win','cdt',array(12340,1234),1001,165001, 50,40,30,20,10,2000,2000,2000 from dummy;



## key,values倒置
select tag, concat_ws(',', collect_set(id)) FROM test LATERAL VIEW explode(b) tmpxx AS tag group by tag;

### 输出到文件
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/pv_gender_sum' select tag, concat_ws(',', collect_set(id)) FROM test LATERAL VIEW explode(b) tmpxx AS tag group by tag;
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/pv_gender_sum' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  select tag, concat_ws(',', collect_set(id)) FROM test LATERAL VIEW explode(b) tmpxx AS tag group by tag;




---------------------------------------------存储格式--------------

#1. 新表  如果原表是 snappy，那么默认输出也是snappy
#2. 设置 gzip有效
#3. 设置 snappy有效
insert into tmp.test_test select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;


set hive.enforce.bucketing=true;
set hive.exec.compress.output=true;
set mapred.output.compress=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
insert into tmp.test_test select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;

set hive.enforce.bucketing=true;
set hive.exec.compress.output=true;
set mapred.output.compress=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.SnappyCodec;
insert into tmp.test_test select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;



#######################################################################################################################################################################
#############################  parquet
#创建parquet table :
create table mytable(a int,b int) STORED AS PARQUET;

#创建带压缩的parquet table:
create table mytable(a int,b int) STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
#如果原来创建表的时候没有指定压缩，后续可以通过修改表属性的方式添加压缩:
ALTER TABLE mytable SET TBLPROPERTIES ('parquet.compression'='SNAPPY');

#或者在写入的时候set parquet.compression=SNAPPY;
#不过只会影响后续入库的数据，原来的数据不会被压缩，需要重跑原来的数据。
#采用压缩之后大概可以降低1/3的存储大小。
#新表
## parquet方式下设置
#1. 新表 parquet格式
#2. 设置 gzip有效，明显减少
#3. 设置 snappy和没有设置一样，没有区别








drop table tmp.test_test1;
CREATE TABLE tmp.test_test1(
  adslot_id string,
  num bigint
)
  stored as parquet
;



insert into tmp.test_test1 select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;

SET hive.exec.compress.output=true;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression=org.apache.hadoop.io.compress.GzipCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
insert into tmp.test_test1 select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;


SET hive.exec.compress.output=true;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.SnappyCodec;
insert into tmp.test_test1 select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;

############################################################################################################################################################

#测试结果
#1. 和stored as parquet区别 ;
drop table tmp.test_test2;
CREATE TABLE tmp.test_test2(
  adslot_id string,
  num bigint
)
  stored as parquet TBLPROPERTIES ('parquet.compression'='GZIP');

show create table tmp.test_test2;
insert into tmp.test_test2 select req_id, ts from ssp.ods_f_request where data_date = 2018090600 ;





----------------------------------------------------------------------
###创建包 creat table
CREATE EXTERNAL TABLE click_rate(
app_BoundId String,
data_date int,
click int,
exposure int,
click_rate DECIMAL(12, 7))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
'/tmp/ad/click_rate'





----------------------------------------------------------------------jdbc连接hiveServer2


public static void main(String[] args) throws ClassNotFoundException {
Class.forName("org.apache.hive.jdbc.HiveDriver");
try{
Connection con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/default","ericens",null);
PreparedStatement sta = con.prepareStatement("select * from test1");
ResultSet result = sta.executeQuery();
while(result.next()){
System.out.println(result.getString(1));
System.out.println(result.getString(2));
}
} catch(SQLException e) {
e.printStackTrace();
}
}
