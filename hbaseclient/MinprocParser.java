package procstats;
import java.io.*;
import java.util.ArrayList;
import java.util.Set;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.DecimalFormat;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class MinprocParser 
{
/*
{"region":17,"isLearning":0,"aggregatorId":6,"bids":[{"bidAmount":0.001612715,"creativeHeight":72,"creativeWidth":100,"advId":6006,"matchingUserSegmentId":2010494,"advIoId":11286,"matchingPageCategoryId":10303,"advLiId":19772}],"pageCategories":[{"categoryId":10303,"categoryWeight":0.0}],"learningPercentage":0.2364742504,"foldPosition":1,"country":100,"city":21,"timestamp":1387020021,"urid":"0ae1a26d08e52f211b86a6e06b442ec950bdc5d569ba9aba46087","siteId":3513,"eligibleLisByType":[{"eligibleLis":[{"eligibleLiId":19772}],"eligibleLiType":2},{"eligibleLis":[],"eligibleLiType":1}],"bidInfoListSize":1,"invalidLL":0,"userCookie":"0010a674-c2db-4b49-be2e-694a64a20da6}
*/
    public static HashMap getAdvIdAndBids(File f)
    {
        byte[] rawData = new byte[(int) f.length()];
        try
        {
            DataInputStream dis = new DataInputStream(new FileInputStream(f));
            dis.readFully(rawData);
            dis.close();
            return getAdvIdAndBids(rawData);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static HashMap getAdvIdAndBids(byte[] rawData)
    {
        try
        {
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            String minProcJsonData=utf8Decoder.decode(ByteBuffer.wrap(rawData)).toString();
            return getAdvIdAndBids(minProcJsonData);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;

    }
    //This DS returns a map where key is URID and the value is stats object which contains advid:bidprice,clicks,imps,convs and auctions. 
    public static HashMap getAdvIdAndBids(String jsonData)
    {
        HashMap<String,URIDCounter> ret=new HashMap<String,URIDCounter>();
        //create ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();
 
        JsonNode rootNode ;
        //read JSON like DOM Parser
        try
        {
            rootNode = objectMapper.readTree(jsonData);
        }catch(java.io.IOException e)
        {
            System.out.println(e.getMessage());
            return null;
        }

        Iterator<JsonNode> records=rootNode.getElements(); 
        while(records.hasNext())
        {
            JsonNode record = records.next();
            JsonNode urIdNode=record.path("urid");
            String urId=urIdNode.getValueAsText();
            long timeStamp=(record.path("timestamp")).getLongValue();
            if(urId.length()<=3)
            {
                urId=new String("NAC");
            }
            //System.out.println("urid="+urId+"len="+urId.length());
            JsonNode bidsNode = record.path("bids");
            Iterator<JsonNode> elements = bidsNode.getElements();
            if(elements.hasNext())
            {
                while(elements.hasNext())
                {
                    JsonNode bid = elements.next();
                    JsonNode bidAmount = bid.path("bidAmount");
                    JsonNode advId = bid.path("advId");
                    DecimalFormat df = new DecimalFormat("0.00000");
                    //String advIdAndBid=new String(advId.getValueAsText()+":"+bidAmount.getValueAsText());
                    String advIdAndBid=advId+":"+df.format(bidAmount.getDoubleValue());
                    URIDCounter uc=ret.get(urId);
                    if(uc==null)
                    {
                        uc=new URIDCounter(urId,advIdAndBid,timeStamp);
                        ret.put(urId,uc);
                    }
                    uc.addAucs(1);
                    uc.addBids(1);
                    //System.out.println("advId:BidAmount= "+advId.toString()+":"+bidAmount.toString());
                }
            }else
            {
                //We didnt participate in so many auctions
                URIDCounter uc=ret.get(urId);
                if(uc==null)
                {
                    uc=new URIDCounter(urId,"0:0",timeStamp);
                    ret.put(uc.getUrid(),uc);
                }
                uc.addAucs(1);
            }
        }
        return ret;
    }
    public static HashMap<String,StatsCounter> getBidLandScapeObject(byte[] minproc,byte[] rtb,
            HashMap<String,StatsCounter> bidMap)
    {
        HashMap<String,URIDCounter> procMap=MinprocParser.getAdvIdAndBids(minproc);
        HashMap<String,URIDCounter> userLs=RtbParser.getAdvIdAndBids(rtb,procMap);
        if(bidMap==null)
        {
            bidMap=new HashMap<String,StatsCounter>();
        }
        if(procMap==null || userLs==null)
        {
            return bidMap;
        }
        Iterator<String> keyIter=userLs.keySet().iterator();
        while(keyIter.hasNext())
        {
            String key=keyIter.next();
            URIDCounter uc=userLs.get(key);
            //System.out.println(key+"..>"+uc.toString());
            String bidString=((uc.getWinningBidPrice()==null)?uc.getBidPrice():uc.getWinningBidPrice());
            /*
            if(uc.getWinningBidPrice()!=null)
            {
                System.out.println("BidPrice="+uc.getBidPrice()+" WinPrice="+uc.getWinningBidPrice());
            }
            */
            StatsCounter sc=bidMap.get(bidString);
            if(sc==null)
            {
                sc=new StatsCounter();
                bidMap.put(bidString,sc);
            }
            String segment=String.valueOf(uc.getDayPart());
            sc.addAucs(segment,uc.getAucs());
            sc.addImps(segment,uc.getImps());
            sc.addClicks(segment,uc.getClicks());
            sc.addConvs(segment,uc.getConvs());
            //sc.addAllConvs(segment,uc.getAllConvs());
            sc.addBids(segment,uc.getBids());
            sc.addWinPercentage(uc.getWinPercent(),1);
            sc.addBidPercentage(uc.getBidPercent(),1);
            //System.out.println(key+"..>"+c.toString());
        }
        return bidMap;
    }
    public static String getBidLandScapeAsJsonString(HashMap<String,StatsCounter> bidMap)
    {
        if(bidMap==null)
            return null;
        Iterator<String> bidMapKeyIter=bidMap.keySet().iterator();
        JsonNodeFactory nf=JsonNodeFactory.instance;
        ArrayNode bidLs=new ArrayNode(nf);
        while(bidMapKeyIter.hasNext())
        {
            String bidPrice=bidMapKeyIter.next();
            StatsCounter sc=bidMap.get(bidPrice);
            ObjectNode data=sc.toJson();
            data.put("advid:bidprice",bidPrice);
            String s[]=bidPrice.split(":");
            data.put("advid",s[0]);
            data.put("bidprice",s[1]);
            bidLs.add(data);
        }
        return bidLs.toString();
    }
    public static String getBidLandScapeAsJsonString(byte[] minproc,byte[] rtb)
    {
        HashMap<String,URIDCounter> procMap=MinprocParser.getAdvIdAndBids(minproc);
        HashMap<String,URIDCounter> userLs=RtbParser.getAdvIdAndBids(rtb,procMap);
        if(procMap==null || userLs==null)
        {
            return "";
        }

        HashMap<String,StatsCounter> bidMap=new HashMap<String,StatsCounter>();
        Iterator<String> keyIter=userLs.keySet().iterator();
        while(keyIter.hasNext())
        {
            String key=keyIter.next();
            URIDCounter uc=userLs.get(key);
            //System.out.println(key+"..>"+uc.toString());
            String bidString=((uc.getWinningBidPrice()==null)?uc.getBidPrice():uc.getWinningBidPrice());
            /*
            if(uc.getWinningBidPrice()!=null)
            {
                System.out.println("BidPrice="+uc.getBidPrice()+" WinPrice="+uc.getWinningBidPrice());
            }
            */
            StatsCounter sc=bidMap.get(bidString);
            if(sc==null)
            {
                sc=new StatsCounter();
                bidMap.put(bidString,sc);
            }
            String segment=String.valueOf(uc.getDayPart());
            sc.addAucs(segment,uc.getAucs());
            sc.addImps(segment,uc.getImps());
            sc.addClicks(segment,uc.getClicks());
            sc.addConvs(segment,uc.getConvs());
            sc.addBids(segment,uc.getBids());
            //System.out.println(key+"..>"+c.toString());
        }
        Iterator<String> bidMapKeyIter=bidMap.keySet().iterator();
        JsonNodeFactory nf=JsonNodeFactory.instance;
        ArrayNode bidLs=new ArrayNode(nf);
        while(bidMapKeyIter.hasNext())
        {
            String bidPrice=bidMapKeyIter.next();
            StatsCounter sc=bidMap.get(bidPrice);
            ObjectNode data=sc.toJson();
            data.put("advid:bidprice",bidPrice);
            String s[]=bidPrice.split(":");
            data.put("advid",s[0]);
            data.put("bidprice",s[1]);
            bidLs.add(data);
        }
        return bidLs.toString();
    }

    public static HashMap getAuctionHistogram(File f, List<Integer> tshwhr,List<ShoppingWindow> shwl)
    {
        byte[] rawData = new byte[(int) f.length()];
        try
        {
            DataInputStream dis = new DataInputStream(new FileInputStream(f));
            dis.readFully(rawData);
            dis.close();
            return getAuctionHistogram(rawData,tshwhr,shwl);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static HashMap getAuctionHistogram(byte[] rawData, List<Integer> tshwhr,List<ShoppingWindow> shwl)
    {
        try
        {
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            String minProcJsonData=utf8Decoder.decode(ByteBuffer.wrap(rawData)).toString();
            return getAuctionHistogram(minProcJsonData,tshwhr,shwl);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;

    }
    

    //This DS returns a map where key is URID and the value is stats object which contains clicks,imps,convs and auctions. 
    public static HashMap getAuctionHistogram(String jsonData,List<Integer> tshwhr,List<ShoppingWindow> shwl)
    {
        HashMap<String,URIDCounter> ret=new HashMap<String,URIDCounter>();
        //create ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();
 
        JsonNode rootNode ;
        //read JSON like DOM Parser
        try
        {
            rootNode = objectMapper.readTree(jsonData);
        }catch(java.io.IOException e)
        {
            System.out.println(e.getMessage());
            return null;
        }
        //boolean hasNotSetAllAucs=true;

        Iterator<JsonNode> records=rootNode.getElements(); 
        while(records.hasNext())
        {
            JsonNode record = records.next();
            JsonNode urIdNode=record.path("urid");
            String urId=urIdNode.getValueAsText();
            long timeStamp=(record.path("timestamp")).getLongValue();
            List<String> segments=ShoppingWindow.getShwSegments(timeStamp,tshwhr,shwl);
            if(segments==null || segments.size()<=0)
                continue;

            //System.out.println("minproc timestamp="+timeStamp);

            if(urId.length()<=3)
            {
                urId=new String("NAC");
            }
            //System.out.println("urid="+urId+"len="+urId.length());
            JsonNode bidsNode = record.path("bids");
            Iterator<JsonNode> elements = bidsNode.getElements();
            if(elements.hasNext())
            {
                while(elements.hasNext())
                {
                    JsonNode bid = elements.next();
                    JsonNode bidAmount = bid.path("bidAmount");
                    JsonNode advId = bid.path("advId");
                    DecimalFormat df = new DecimalFormat("0.00000");
                    //String advIdAndBid=new String(advId.getValueAsText()+":"+bidAmount.getValueAsText());
                    String advIdAndBid=advId+":"+df.format(bidAmount.getDoubleValue());
                    URIDCounter uc=ret.get(urId);
                    if(uc==null)
                    {
                        uc=new URIDCounter(urId,advIdAndBid,timeStamp);
                        ret.put(urId,uc);
                        uc.setSegments(segments);
                    }
                    uc.addAucs(1);
                    uc.addBids(1);
                    /*
                    if(hasNotSetAllAucs)
                    {
                        uc.addAllConvs(shwl.size());
                        hasNotSetAllAucs=false;
                    }
                    */
                    //System.out.println("advId:BidAmount= "+advId.toString()+":"+bidAmount.toString());
                }
            }else
            {
                //We didnt participate in so many auctions
                URIDCounter uc=ret.get(urId);
                if(uc==null)
                {
                    uc=new URIDCounter(urId,"0:0",timeStamp);
                    ret.put(uc.getUrid(),uc);
                    uc.setSegments(segments);
                }
                uc.addAucs(1);
                /*
                if(hasNotSetAllAucs)
                {
                    uc.addAllConvs(shwl.size());
                    hasNotSetAllAucs=false;
                }
                */
            }
        }
        return ret;
    }

    public static HashMap getAuctionHistObject(byte[] minproc,byte[] rtb, List<Integer> tshwhr,
        List<ShoppingWindow> shwl)
    {
        HashMap<String,URIDCounter> procMap=MinprocParser.getAuctionHistogram(minproc,tshwhr,shwl);
        HashMap<String,URIDCounter> userLs=RtbParser.getAuctionHistogram(rtb,tshwhr,shwl,procMap);
        HashMap<String,StatsCounter> auctMap=new HashMap<String,StatsCounter>();
        //System.out.println("procMapSize="+procMap.size()+"\tuserLs="+userLs.size());
        if(procMap==null || userLs==null )
        {
            System.out.println("procMap or userLs is null");
            return auctMap;
        }

        //String shwInHr=String.valueOf(tshwhr);
        Iterator<String> keyIter=userLs.keySet().iterator();
        while(keyIter.hasNext())
        {
            String key=keyIter.next();
            URIDCounter uc=userLs.get(key);
            //System.out.println(key+"..>"+uc.toString());
            //String advId=uc.getBidPrice().split(":");
            /*
            if(uc.getWinningBidPrice()!=null)
            {
                System.out.println("BidPrice="+uc.getBidPrice()+" WinPrice="+uc.getWinningBidPrice());
            }
            */
            List<String> segl=uc.getSegments();
            if(segl==null || segl.size()<=0)
                continue;

            Iterator<String> segIter=segl.iterator();
            while(segIter.hasNext())
            {
                String segment=segIter.next();
                StatsCounter sc=auctMap.get(segment);
                if(sc==null)
                {
                    sc=new StatsCounter();
                    auctMap.put(segment,sc);
                }
                sc.addAucs(segment,uc.getAucs());
                sc.addImps(segment,uc.getImps());
                sc.addClicks(segment,uc.getClicks());
                sc.addConvs(segment,uc.getConvs());
                sc.addBids(segment,uc.getBids());
            }
            //System.out.println(key+"..>"+c.toString());
        }
        //Add All conversions and uniques
        Iterator<String> auctMapIter=auctMap.keySet().iterator();
        while(auctMapIter.hasNext())
        {
            String segment=auctMapIter.next();
            StatsCounter sc=auctMap.get(segment);
            double winPercent=(double)sc.mImps/(double)sc.mBids;
            double bidPercent=(double)sc.mBids/(double)sc.mAucs;
            sc.addWinPercentage(winPercent,1);
            sc.addBidPercentage(bidPercent,1);
            sc.addUniques(segment,1);
            Iterator<ShoppingWindow> shwlIter=shwl.iterator();
            while(shwlIter.hasNext())
            {
                ShoppingWindow shw=shwlIter.next();
                if(shw.shwhr<=Integer.parseInt(segment))
                {
                    sc=auctMap.get(segment);
                    sc.addAllConvs(segment,1);
                }
            }
        }
        return auctMap;
    }

    //Merge two stats counter objects. Modifies m2 

    public static HashMap mergeStatsCounter(HashMap<String,StatsCounter> m1,HashMap<String,StatsCounter> m2)
    {
        Iterator<String> m1Iter=m1.keySet().iterator();
        while(m1Iter.hasNext())
        {
            String m1Key=m1Iter.next();            
            StatsCounter m1Sc=m1.get(m1Key);
            StatsCounter sc=null;
            if(m2.containsKey(m1Key))
            {
                sc=m2.get(m1Key);
            }else
            {
                sc=new StatsCounter();
            }
            sc.addAucs(m1Key,m1Sc.mAucs);
            sc.addImps(m1Key,m1Sc.mImps);
            sc.addClicks(m1Key,m1Sc.mClicks);
            sc.addConvs(m1Key,m1Sc.mConvs);
            sc.addAllConvs(m1Key,m1Sc.mAllConvs);
            sc.addUniques(m1Key,m1Sc.mUniques);
            sc.addBids(m1Key,m1Sc.mBids);
            
            Iterator<Double> winIter=m1Sc.mWinPercent.keySet().iterator();
            while(winIter.hasNext())
            {
                Double threshold=winIter.next();
                sc.addWinPercentage(threshold,m1Sc.mWinPercent.get(threshold));
            }

            Iterator<Double> bidIter=m1Sc.mBidPercent.keySet().iterator();
            while(bidIter.hasNext())
            {
                Double threshold=bidIter.next();
                sc.addBidPercentage(threshold,m1Sc.mBidPercent.get(threshold));
            }

            m2.put(m1Key,sc);
        }
        return m2;
    }

    public static String getAuctHistAsJsonString(HashMap<String,StatsCounter> auctMap)
    {
        if(auctMap==null)
            return null;
        Iterator<String> auctMapKeyIter=auctMap.keySet().iterator();
        JsonNodeFactory nf=JsonNodeFactory.instance;
        ArrayNode bidLs=new ArrayNode(nf);
        while(auctMapKeyIter.hasNext())
        {
            String shwhr=auctMapKeyIter.next();
            StatsCounter sc=auctMap.get(shwhr);
            ObjectNode data=sc.toJson();
            data.put("shwhr",shwhr);
            bidLs.add(data);
        }
        return bidLs.toString();
    }


 
	public static void main(String[] args) throws JsonParseException, IOException 
	{
        byte[] minproc = null;
        byte[] rtb = null;
        //String shwjson=new String("[{\"start\":1385721390,\"end\":1385721629,\"shwhr\":1},{\"start\":1385993383,\"end\":1386179007,\"shwhr\":52},{\"start\":1387056093,\"end\":1387216706,\"shwhr\":45}]");
        String shwjson=new String("[{\"start\":1385894358,\"end\":1385896295,\"shwhr\":1}]");
        try
        {
            File minProcFile = new File("/tmp/minproc.json");
            File rtbFile = new File("/tmp/rtb.json");
            minproc = new byte[(int) minProcFile.length()];
            rtb = new byte[(int) rtbFile.length()];

            DataInputStream dis = new DataInputStream(new FileInputStream(minProcFile));
            dis.readFully(minproc);
            dis.close();

            dis = new DataInputStream(new FileInputStream(rtbFile));
            dis.readFully(rtb);
            dis.close();

        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        List<ShoppingWindow> shwl=new ArrayList<ShoppingWindow>();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode=null;
        //read JSON like DOM Parser
        try
        {
            rootNode = objectMapper.readTree(shwjson);
        }catch(java.io.IOException e)
        {
            System.out.println(e.getMessage());
        }

        Iterator<JsonNode> records=rootNode.getElements(); 
        while(records.hasNext())
        {
            JsonNode record = records.next();
            long start=record.path("start").getLongValue();
            long end=record.path("end").getLongValue();
            int shwhr=record.path("shwhr").getIntValue();
            ShoppingWindow shw=new ShoppingWindow();
            shw.start=start;
            shw.end=end;
            shw.shwhr=shwhr;
            shwl.add(shw);
        }

        List<Integer>  segments=new ArrayList<Integer>();
        segments.add(1);
        segments.add(3);
        segments.add(5);
        segments.add(24);
        segments.add(48);
        segments.add(30*24);

        HashMap<String,StatsCounter> auctMap= getAuctionHistObject(minproc,rtb,segments,shwl);
        //String auctLs=getAuctHistAsJsonString(minproc,rtb,segments,shwl);
        String auctLs=getAuctHistAsJsonString(auctMap);
        System.out.println(auctLs);
        
        //HashMap<String,StatsCounter> bidMap=getBidLandScapeObject(minproc,rtb,null);
        //String bls=getBidLandScapeAsJsonString(bidMap);
        //System.out.println(bls);
    }
}
