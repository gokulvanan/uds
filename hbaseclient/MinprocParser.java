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
                    DecimalFormat df = new DecimalFormat("0.0000000");
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
    public static HashMap<String,StatsCounter> getBidLandScapeAsJson(byte[] minproc,byte[] rtb,
            HashMap<String,StatsCounter> bidMap)
    {
        if(bidMap==null)
        {
            bidMap=new HashMap<String,StatsCounter>();
        }

    }
    public static String getBidLandScapeAsJson(byte[] minproc,byte[] rtb)
    {
        HashMap<String,URIDCounter> procMap=MinprocParser.getAdvIdAndBids(minproc);
        HashMap<String,URIDCounter> userLs=RtbParser.getAdvIdAndBids(rtb,procMap);

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
 
	public static void main(String[] args) throws JsonParseException, IOException 
	{
        byte[] minproc = null;
        byte[] rtb = null;
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
        String bls=getBidLandScape(minproc,rtb);
        System.out.println(bls);
    }
}
