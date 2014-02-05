REGISTER '/usr/lib/hbase/hbase-0.90.4-cdh3u3.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.5-cdh3u6.jar'
REGISTER './logp-common-1.7.jar'
REGISTER './pig-to-json.jar'
REGISTER './json_simple-1.1.jar'

--Read the new logfile

raw = load '/amt/2013/12/21/19/00/logs/pixel_raw/current/PXS8-pixel-requests.20131221192500' using PigStorage('\t') as (formatId:int, versionid:int, urid:chararray, pixelId:chararray,userid:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int, status:int);

fraw = filter raw by userid is not null and userid!='null';
describe fraw;

--Get unique userids from newlogfile
ids = FOREACH fraw GENERATE userid as id;
describe ids;
activeids = DISTINCT ids;
describe activeids;
--dump activeids;

--Read Hbase table

fcurlog = load 'hbase://smalluser' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt','-loadKey true') AS (id:chararray,amt:bag{T:tuple(formatId:int, versionid:int, urid:chararray, pixelId:chararray,userid:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int, status:int)}); 
describe fcurlog;

common = COGROUP fcurlog by id INNER, activeids BY id INNER;
describe common;
--dump common;

--Flatten twice; Once for removing cogroup bag and second one for getting individual log entries
grpdcmn = FOREACH common GENERATE FLATTEN(fcurlog.amt) AS amt;
describe grpdcmn;
cmnlog = FOREACH grpdcmn GENERATE FLATTEN(amt) ;
describe cmnlog;
--dump cmnlog;

--Merge old and new entries
merged= UNION fraw,cmnlog;
describe merged;

--Stream just to add schema
decoratedLog = STREAM merged THROUGH `/bin/cat ` AS (formatId:int, versionid:int, urid:chararray, pixelId:chararray,userid:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int, status:int);
describe decoratedLog;
--dump decoratedLog;

--Convert log entries to bag
grpdlog =  group decoratedLog by userid;
describe grpdlog;

--Remove duplicate log entries and order by timestamp
data = FOREACH grpdlog {
    Y = DISTINCT decoratedLog;
	X = ORDER Y BY timestamp;
	generate group,SIZE(X) as amt_rowcnt,MIN(X.timestamp) as mintime,MAX(X.timestamp) as maxtime,X as amt,com.hortonworks.pig.udf.ToJson(X) as amtjson;
	};
describe data;
--dump data;

--Store it back in hbase

STORE data INTO 'hbase://smalluser' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt_rowcnt r:amt_mintime r:amt_maxtime r:amt r:amtjson');

