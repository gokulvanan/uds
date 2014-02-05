REGISTER '/usr/lib/hbase/hbase-0.90.4-cdh3u3.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.5-cdh3u6.jar'
REGISTER './logp-common-1.7.jar'
REGISTER './pig-to-json.jar'
REGISTER './json_simple-1.1.jar'

--Read the new logfile

raw = load '/amt/2013/12/21/19/00/logs/pixel_raw/current/PXS8-pixel-requests.20131221192500' using PigStorage('\t') as (f1:chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,f6: long,f7: chararray,f8: chararray,f9: chararray,f10: chararray,f11: chararray,f12: chararray,f13: chararray,f14: chararray,f15: chararray,f16: chararray,f17: chararray,f18: chararray,f19: chararray);

fraw = filter raw by f5 is not null and f5!='null';
describe fraw;

--Get unique userids from newlogfile
ids = FOREACH fraw GENERATE f5 as id;
describe ids;
activeids = DISTINCT ids;
describe activeids;
--dump activeids;

--Read Hbase table

fcurlog = load 'hbase://smalluser' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt','-loadKey true') AS (id:chararray, amt:bag{T:tuple(f1:chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,f6: long,f7: chararray,f8: chararray,f9: chararray,f10: chararray,f11: chararray,f12: chararray,f13: chararray,f14: chararray,f15: chararray,f16: chararray,f17: chararray,f18: chararray,f19: chararray)});

--fcurlog = FILTER curlog BY not IsEmpty(amt);
describe fcurlog;

common = COGROUP fcurlog by id INNER, activeids BY id INNER;
describe common;
--dump common;

grpdcmn = FOREACH common {
    X=fcurlog.amt;
    GENERATE FLATTEN(X) AS amt;
    };
describe grpdcmn;
--dump grpdcmn;

cmnlog = FOREACH grpdcmn GENERATE FLATTEN(amt) ;
describe cmnlog;
--dump cmnlog;

--Merge old and new entries
merged= UNION fraw,cmnlog;
describe merged;

--Stream just to add schema
decoratedLog = STREAM merged THROUGH `/bin/cat ` AS (f1:chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,f6: long,f7: chararray,f8: chararray,f9: chararray,f10: chararray,f11: chararray,f12: chararray,f13: chararray,f14: chararray,f15: chararray,f16: chararray,f17: chararray,f18: chararray,f19: chararray);
describe decoratedLog;
--dump decoratedLog;

grpdlog =  group decoratedLog by f5;
describe grpdlog;

data = FOREACH grpdlog {
    Y = DISTINCT decoratedLog;
	X = ORDER Y BY f6;
	generate group,SIZE(X) as amt_rowcnt,MIN(X.f6) as mintime,MAX(X.f6) as maxtime,X as amt,com.hortonworks.pig.udf.ToJson(X) as amtjson;
	};
describe data;
--dump data;

--Store it back in hbase

STORE data INTO 'hbase://smalluser' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt_rowcnt r:amt_mintime r:amt_maxtime r:amt r:amtjson');

