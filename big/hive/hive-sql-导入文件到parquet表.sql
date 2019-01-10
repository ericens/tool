


###############################################################################################################################################
#### 导入文件到parquet表，或者装换文件格式
###############################################################################################################################################

#0. 穿件textfile表
drop table tmp.sample_data;
create table tmp.sample_data like ad.ad_sample_data stored as textFile;

#1. 导入 text表

hive -S -e "load data local inpath './000000_0'   into table  tmp.sample_data  partition(data_date=2018091555)"
hive -S -e "load data local inpath './000001_0'   into table  tmp.sample_data  partition(data_date=2018091555)"
hive -S -e "load data local inpath './000002_0'   into table  tmp.sample_data  partition(data_date=2018091555)"

#2. 查看文件
hdfs://nameservice1/user/hive/warehouse/tmp.db/sample_data/data_date=2018091555

#3. insert目的parquet表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  ad.ad_sample_data partition(data_date)
select * from tmp.sample_data where data_date=2018091555;