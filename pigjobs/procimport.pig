REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'
REGISTER './logp-common-1.7.jar'


--raw = LOAD '/user/atom/atom/raw/processor_minified/2013/12/*/*/*.lzo' USING net.atomex.logprocessor.pigutils.MinifiedProcessorLogLoader();

raw = LOAD '/user/atom/atom/raw/processor_minified/2013/12/1/1/part-r-00000.lzo' USING net.atomex.logprocessor.pigutils.MinifiedProcessorLogLoader();
describe raw;

myntrausers = LOAD '/user/siddharth/myntraUsers1/*' USING PigStorage('\t') AS (userId:chararray);

fusers = join raw by userCookie ,myntrausers by userId USING 'replicated' ;
describe fusers;

d = foreach fusers generate  raw::invalidLL,raw::timestamp,raw::aggregatorId,raw::urid,raw::isLearning,raw::userCookie,raw::country,raw::region,raw::city,raw::siteId,raw::foldPosition,raw::learningPercentage,raw::pageCategories,raw::bidInfoListSize,raw::bids,raw::eligibleLisByType;
describe d;

STORE d  INTO '/user/sbalasubramanian/minproc' USING PigStorage('\t');

