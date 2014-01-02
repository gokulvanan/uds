REGISTER '/usr/lib/hbase/lib/jackson-core-asl-1.5.2.jar'
REGISTER '/usr/lib/hbase/lib/jackson-mapper-asl-1.5.2.jar'
REGISTER '/usr/lib/oozie/lib/json-simple-1.1.jar'
REGISTER './piggybank.jar'
REGISTER './elephat
REGISTER './avro-1.7.5.jar'
REGISTER './avro-tools-1.7.5.jar'
REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'
REGISTER './logp-common-1.7.jar'


ddata = LOAD '/user/sbalasubramanian/ddata.small' USING PigStorage('\t') AS (id:chararray,minproc_rowcnt:long,minproc_mintime:long,minproc_maxtime:long,minproc:bag{T:tuple(invalidLL: int,timestamp: long,aggregatorId: int,urid: chararray,isLearning: int,userCookie: chararray,country: int,region: int,city: int,siteId: long,foldPosition: int,learningPercentage: double,pageCategories: {t: (categoryId: int,categoryWeight: double)},bidInfoListSize: int,bids: {t: (advLiId: long,advIoId: long,advId: long,bidAmount: double,matchingPageCategoryId: int,creativeHeight: int,creativeWidth: int,matchingUserSegmentId: int)},eligibleLisByType: {t: (eligibleLiType: int,eligibleLis: {t: (eligibleLiId: int)})})});

--fdata = filter ddata by id=='0010a674-c2db-4b49-be2e-694a64a20da6' or id=='0000e26c-6b90-4ac9-92d2-167aa5772b63' or id=='00011718-ff5a-4dc3-82a8-efdcaed8e90b' or id=='00012bd3-cdf2-4851-9d15-8f447d1e5fc0' or id=='00015a6f-c9d9-4be7-a80f-f9c8cf854239';

STORE ddata INTO '/user/sbalasubramanian/ddata.avro' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
