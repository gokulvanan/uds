REGISTER '/usr/lib/hbase/hbase-0.90.3-cdh3u1.jar'
REGISTER '/usr/lib/hbase/lib/guava-r06.jar'
REGISTER '/usr/lib/zookeeper/zookeeper-3.3.3-cdh3u1.jar'

fcurlog = load 'hbase://smalluser' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('r:amt r:dco r:rtb','-loadKey true') AS (id:chararray,  amt:bag{T:tuple(formatId:int, versionid:int, urid:chararray, pixelId:chararray,userid:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int, status:int)},dco:bag{T1:tuple(formatId:int, versionid:int, userId:chararray, timestamp:long, pageURL:chararray, referrelURL:chararray, userAgent: chararray, ip:chararray, countryId: int, regionId: int, cityId: int, paramString:chararray, reqType: chararray, pixelType: chararray, tagType: chararray, responseTimeInMS: int,partnerId: int, businessVertical:chararray, action:chararray, jsonString: chararray)},rtb:bag{T2:tuple(invalidLL: int,invalidLLForReporting: int,isClickvalidationFailed: int,isViewConv: int,isClickConv: int,invalidLF: int,browserId: int,timestamp: long,isLearning: int,processorId: int,sourceComponent: int,aggregatorId: int,requestUri: chararray,userCookie: (userId: chararray,timestamp: long),userIp: chararray,countryId: int,regionId: int,cityId: int,metroId: int,dma: int,timezone: int,userAgent: chararray,publisherId: long,siteId: long,foldPosition: int,referrer: chararray,uniqueResponseId: chararray,uniqueRowId: chararray,trackerId: int,dataVersion: long,totalProcessingTime: long,initializationTime: long,requestParsingTime: long,requestProcessingTime: long,advertiserId: long,advertiserIoId: long,advertiserLiId: long,creativeId: long,inventorySourceId: long,bidStrategyTypeId: int,pixelId: long,contentCategoryId: int,userSegmentId: int,creativeWidth: int,creativeHeight: int,bucketizedCreativeHeight: int,bucketizedCreativeWidth: int,bucketizedAdvertiserLiFrequency: int,trackingType: int,winningBidPrice: double,impressions: long,conversions: long,clicks: long,creativeOfferType: int,moneySpent: double,licenseeCost: double,licenseeRevenue: double,platformCost: double,platformRevenue: double,statusCode: int,impressionClickValidationStatusCode: int,advertiserTargetingExpression: chararray,userSegments: {t: (segmentId: int,weight: float)},conversionType: int,creativeViewFrequency: int,creativeClickFrequency: int,advertiserIoViewFrequency: int,advertiserIoClickFrequency: int,impressionPiggybackPixels: {t: (id: int)},creativeViewFrequencyOld: int,dataCenterId: int,adSpotId: long,pageType: int,numSlots: int,slotPosition: int,debugStatusCode: long,targetedSegments: {t: (id: int)},blockedSegments: {t: (id: int)})});

describe fcurlog;
amtentries = foreach fcurlog generate id,amt;





