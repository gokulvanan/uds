REGISTER '/usr/lib/hbase/hbase-0.90.4-cdh3u3.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.5-cdh3u6.jar'
--REGISTER './logp-common-1.7.jar'
REGISTER './pig-to-json.jar'
REGISTER './json_simple-1.1.jar'

curlog = load 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt','-loadKey true') AS (id:chararray,  amt:bag{T:tuple(formatId:int, versionid:int, urid:chararray, pixelId:chararray,userid:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int, status:int)});
describe curlog;

fcurlog = FILTER curlog BY (amt IS NOT NULL) AND (NOT IsEmpty(amt)) PARALLEL 100;
describe fcurlog;

amtjson = foreach fcurlog generate id,com.hortonworks.pig.udf.ToJson(amt) as amtjson;
describe amtjson;

store amtjson into 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amtjson');

