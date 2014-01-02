REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'

curlog = load 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt','-loadKey true') AS (id:chararray, amtlog:bag{T:tuple(f1:chararray,f2: chararray,f3: bytearray,f4: bytearray,f5: chararray,f6: bytearray,f7: bytearray,f8: bytearray,f9: bytearray,f10: bytearray,f11: bytearray,f12: bytearray,f13: bytearray,f14: bytearray,f15: bytearray,f16: bytearray,f17: bytearray,f18: bytearray,f19: bytearray)});
describe curlog;

fcurlog = filter curlog by id=='32114979-2d89-11e3-a02f-3cd92becb9e4' or id=='4a9b16a6-2a89-11e2-9d55-001b21c5044c' or id=='1c791980-39bc-11e3-a02f-3cd92becb9e4';
describe fcurlog;
--dump fcurlog;

frawcurlog = foreach fcurlog generate id,SIZE(amtlog) as nrows;
describe frawcurlog;
dump frawcurlog;


