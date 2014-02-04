REGISTER '/usr/lib/hbase/hbase-0.90.4-cdh3u3.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.5-cdh3u6.jar'
--REGISTER './logp-common-1.7.jar'
REGISTER './pig-to-json.jar'
REGISTER './json_simple-1.1.jar'

fcurlog = load 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:rtb','-loadKey true') AS (id:chararray,rtb:bag{T2:tuple(invalidLL: int,invalidLLForReporting: int,isClickvalidationFailed: int,isViewConv: int,isClickConv: int,invalidLF: int,browserId: int,timestamp: long,isLearning: int,processorId: int,sourceComponent: int,aggregatorId: int,requestUri: chararray,userCookie: (userId: chararray,timestamp: long),userIp: chararray,countryId: int,regionId: int,cityId: int,metroId: int,dma: int,timezone: int,userAgent: chararray,publisherId: long,siteId: long,foldPosition: int,referrer: chararray,uniqueResponseId: chararray,uniqueRowId: chararray,trackerId: int,dataVersion: long,totalProcessingTime: long,initializationTime: long,requestParsingTime: long,requestProcessingTime: long,advertiserId: long,advertiserIoId: long,advertiserLiId: long,creativeId: long,inventorySourceId: long,bidStrategyTypeId: int,pixelId: long,contentCategoryId: int,userSegmentId: int,creativeWidth: int,creativeHeight: int,bucketizedCreativeHeight: int,bucketizedCreativeWidth: int,bucketizedAdvertiserLiFrequency: int,trackingType: int,winningBidPrice: double,impressions: long,conversions: long,clicks: long,creativeOfferType: int,moneySpent: double,licenseeCost: double,licenseeRevenue: double,platformCost: double,platformRevenue: double,statusCode: int,impressionClickValidationStatusCode: int,advertiserTargetingExpression: chararray,userSegments: {t: (segmentId: int,weight: float)},conversionType: int,creativeViewFrequency: int,creativeClickFrequency: int,advertiserIoViewFrequency: int,advertiserIoClickFrequency: int,impressionPiggybackPixels: {t: (id: int)},creativeViewFrequencyOld: int,dataCenterId: int,adSpotId: long,pageType: int,numSlots: int,slotPosition: int,debugStatusCode: long,targetedSegments: {t: (id: int)},blockedSegments: {t: (id: int)})});
describe fcurlog;

rtbjson = foreach fcurlog generate id,com.hortonworks.pig.udf.ToJson(rtb) as rtbjson;
describe rtbjson;
store rtbjson into 'hbase://user' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:rtbjson');

