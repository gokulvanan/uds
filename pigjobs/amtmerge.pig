REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'

--Read the new logfile

raw = load '/amt/2013/12/21/19/00/logs/pixel_raw/current/PXS8-pixel-requests.20131221192500' using PigStorage('\t') as (f1:chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,f6: long,f7: chararray,f8: chararray,f9: chararray,f10: chararray,f11: chararray,f12: chararray,f13: chararray,f14: chararray,f15: chararray,f16: chararray,f17: chararray,f18: chararray,f19: chararray);

fraw = filter raw by f5 is not null and f5!='null';
describe fraw;

--Get userids from newlogfile
grpdraw = GROUP fraw by f5 ;
describe grpdraw;
activeids = FOREACH grpdraw GENERATE group as id;
describe activeids;
--dump activeids;

--Read Hbase table

curlog = load 'hbase://smalluser' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt','-loadKey true') AS (id:chararray, amt:bag{T:tuple(f1:chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,f6: long,f7: chararray,f8: chararray,f9: chararray,f10: chararray,f11: chararray,f12: chararray,f13: chararray,f14: chararray,f15: chararray,f16: chararray,f17: chararray,f18: chararray,f19: chararray)});
describe curlog;

fcurlog = FILTER curlog BY not IsEmpty(amt);
describe fcurlog;

--Extract only userids for which there is newer entries
common = COGROUP activeids by id inner ,fcurlog by id inner ;
describe common;
--dump common;

jfrawcurlog = FOREACH common GENERATE  FLATTEN(fcurlog);
describe jfrawcurlog;
--dump jfrawcurlog;

cmnlog = FOREACH jfrawcurlog GENERATE FLATTEN(amt);
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
	generate group,SIZE(X) as amt_rowcnt,MIN(X.f6) as mintime,MAX(X.f6) as maxtime,X as amt;
	};
describe data;
dump data;

--Store it back in hbase

--STORE data INTO 'hbase://smalluser' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt_rowcnt r:amt_mintime r:amt_maxtime r:amt');


