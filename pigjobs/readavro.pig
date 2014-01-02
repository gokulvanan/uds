REGISTER '/usr/lib/hbase/lib/jackson-core-asl-1.5.2.jar'
REGISTER '/usr/lib/hbase/lib/jackson-mapper-asl-1.5.2.jar'
REGISTER '/usr/lib/oozie/lib/json-simple-1.1.jar'
REGISTER './piggybank.jar'
REGISTER './avro-1.7.5.jar'
REGISTER './avro-tools-1.7.5.jar'
REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'
REGISTER './logp-common-1.7.jar'


--ddata = LOAD '/user/sbalasubramanian/ddata.small' USING PigStorage('\t') AS (id:chararray,minproc_rowcnt:long,minproc_mintime:long,minproc_maxtime:long,minproc:bag{T:tuple(invalidLL: int,timestamp: long,aggregatorId: int,urid: chararray,isLearning: int,userCookie: chararray,country: int,region: int,city: int,siteId: long,foldPosition: int,learningPercentage: double,pageCategories: {t: (categoryId: int,categoryWeight: double)},bidInfoListSize: int,bids: {t: (advLiId: long,advIoId: long,advId: long,bidAmount: double,matchingPageCategoryId: int,creativeHeight: int,creativeWidth: int,matchingUserSegmentId: int)},eligibleLisByType: {t: (eligibleLiType: int,eligibleLis: {t: (eligibleLiId: int)})})});

raw = LOAD '/user/sbalasubramanian/avro-ddata' USING PigStorage('\t');

ddata = stream raw through org.apache.avro.mapred.AvroAsTextInputFormat();
describe ddata;

dump ddata;


