REGISTER '/usr/lib/hbase/hbase-0.90.4-cdh3u3.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.5-cdh3u6.jar'
--REGISTER './logp-common-1.7.jar'
REGISTER './pig-to-json.jar'
REGISTER './json_simple-1.1.jar'

curlog = load 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:dco','-loadKey true') AS (id:chararray,dco:bag{T1:tuple(formatId:int, versionid:int, userId:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int,partnerId: int, businessVertical:chararray, action:chararray, jsonString: chararray)});
describe curlog;

fcurlog = FILTER curlog BY (dco IS NOT NULL) AND (NOT IsEmpty(dco)) PARALLEL 100;
describe fcurlog;

dcojson = foreach fcurlog generate id,com.hortonworks.pig.udf.ToJson(dco) as dcojson;
describe dcojson;
store dcojson into 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:dcojson');

