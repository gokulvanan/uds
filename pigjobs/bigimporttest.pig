REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'

raw = load '/amt/2013/10/12/00/00/sys_state/global/gust/current/part-r-00014.lzo' using PigStorage('\t') as (f1:chararray,f2:chararray,f3,f4,id:chararray,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19);
rraw = sample raw 0.02 ;
data = group raw all PARALLEL 100;
describe data;
ddata = foreach data generate 1 as rowkey,raw as raw;
describe ddata;
store data into 'hbase://small_user' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt');

