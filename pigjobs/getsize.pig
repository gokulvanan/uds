REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'

curlog = load 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt','-loadKey true') AS (id:chararray,amt:chararray); 
describe curlog;

fcurlog = foreach curlog generate id,SIZE(amt) as size;
describe fcurlog;
--dump fcurlog;

orderedLog = ORDER fcurlog BY size DESC PARALLEL 100; 
describe orderedLog;
dump orderedLog;


