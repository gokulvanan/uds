package procstats;
import java.util.*;
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
public class StatsCounter 
{
    //A simple structure to segment the stats according to any criteria
    //The overall stats then is just a sum over all Stats Structure
    //This is overall impressions, clicks, conversions, bids and auctions
    public int mImps,mClicks,mConvs,mBids,mAucs,mAllConvs,mUniques;
    public HashMap<Double,Integer> mWinPercent;
    public HashMap<Double,Integer> mBidPercent;
    public HashMap<Double,Integer> mWinningBidPrice;
    ArrayList<Double> mPercentKey;
    public String mShwhr;  
    //HashMap<String,Stats> mStatsMap;

    public StatsCounter()
    {
        //mStatsMap=new HashMap<String,Stats>();
        mShwhr=null;
        mImps=mClicks=mConvs=mBids=mAucs=mAllConvs=mUniques=0;
        mPercentKey=new ArrayList<Double>();
        mPercentKey.add(new Double(0.0));
        mPercentKey.add(new Double(0.05));
        mPercentKey.add(new Double(0.10));
        mPercentKey.add(new Double(0.25));
        mPercentKey.add(new Double(0.75));
        mPercentKey.add(new Double(1.0));
        mWinPercent=new HashMap<Double,Integer>();
        mBidPercent=new HashMap<Double,Integer>();
        Iterator<Double> percentIter=mPercentKey.iterator();
        while(percentIter.hasNext())
        {
            Double percent=percentIter.next();
            mWinPercent.put(percent,0);
            mBidPercent.put(percent,0);
        }
    }
    public void addWinPercentage(double winPercent,int cnt)
    {
        //System.out.println("win%="+winPercent);
        Iterator<Double> winIter=mPercentKey.iterator();
        while(winIter.hasNext())
        {
            Double threshold=winIter.next();
            if(winPercent<=threshold)
            {
                Integer Count=mWinPercent.get(threshold);
                Count+=cnt;
                mWinPercent.put(threshold,Count);
            }
        }
    }
    public void addBidPercentage(double bidPercent,int cnt)
    {
        //System.out.println("Bid%="+bidPercent);
        Iterator<Double> bidIter=mPercentKey.iterator();
        while(bidIter.hasNext())
        {
            Double threshold=bidIter.next();
            if(bidPercent<=threshold)
            {
                Integer Count=mBidPercent.get(threshold);
                Count+=cnt;
                mBidPercent.put(threshold,Count);
            }
        }
    }
    public void addBids(int cnt)
    {
        mBids+=cnt;
    }
    public void addUniques(int cnt)
    {
        mUniques+=cnt;
    }
    public void addImps(int cnt)
    {
        mImps+=cnt;
    }
    public void addClicks(int cnt)
    {
       mClicks+=cnt;
    }
    public void addAucs(int cnt)
    {
        mAucs+=cnt;
    }
    public void addAllConvs(int cnt)
    {
        mAllConvs+=cnt;
    }
    public void addConvs(int cnt)
    {
        mConvs+=cnt;
    }
    public void setShwhr(String shwhr)
    {
        mShwhr=new String(shwhr);
    }


    public static HashMap<String,StatsCounter> fromJson(byte[] rawData)
    {
        HashMap<String,StatsCounter> ret=new HashMap<String,StatsCounter>();
        ObjectMapper objectMapper = new ObjectMapper();
 
        JsonNode rootNode ;
        //read JSON like DOM Parser
        try
        {
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            String jsonData=utf8Decoder.decode(ByteBuffer.wrap(rawData)).toString();
            rootNode = objectMapper.readTree(jsonData);
        }catch(java.io.IOException e)
        {
            System.out.println(e.getMessage());
            return null;
        }
        System.out.println("Iterating over elements");
        Iterator<JsonNode> records=rootNode.getElements(); 
        while(records.hasNext())
        {
            StatsCounter sc=new StatsCounter();
            JsonNode record = records.next();
            sc.mAllConvs=record.path("allconvs").getIntValue();
            sc.mConvs=record.path("convs").getIntValue();
            sc.mClicks=record.path("clicks").getIntValue();
            sc.mImps=record.path("imps").getIntValue();
            sc.mBids=record.path("bids").getIntValue();
            sc.mAucs=record.path("aucs").getIntValue();
            sc.mUniques=record.path("uniques").getIntValue();
            Iterator<JsonNode> elements=record.path("WinBidPercent").getElements();
            while(elements.hasNext())
            {
                JsonNode winBidPercent = elements.next();
                //System.out.println(winBidPercent);
                if(winBidPercent.path("winPercent").isMissingNode())
                {
                    //If Winpercent is mising then BidPercent will be present
                    Double bidPercent=new Double(winBidPercent.path("bidPercent").getDoubleValue());
                    Integer bidCount=new Integer(winBidPercent.path("bidCount").getIntValue());
                    sc.mBidPercent.put(bidPercent,bidCount);
                    //System.out.println("bidPercent="+bidPercent+" bidCnt="+bidCount);
                }else
                {
                    Double winPercent=new Double(winBidPercent.path("winPercent").getDoubleValue());
                    Integer winCount=new Integer(winBidPercent.path("winCount").getIntValue());
                    sc.mWinPercent.put(winPercent,winCount);
                    //System.out.println("winPercent="+winPercent+" winCnt="+winCount);
                }
            }
            sc.setShwhr(record.path("shwhr").getTextValue());
            ret.put(sc.mShwhr,sc);
        }
        return ret;
    }

    public ObjectNode toJson()
    {
        try
        {
            JsonNodeFactory nf=JsonNodeFactory.instance;
            ObjectNode all=nf.objectNode();
            all.put("allconvs",mAllConvs);
            all.put("convs",mConvs);
            all.put("clicks",mClicks);
            all.put("imps",mImps);
            all.put("bids",mBids);
            all.put("aucs",mAucs);
            all.put("uniques",mUniques);
            ArrayNode winNodes=new ArrayNode(nf);
            Iterator<Double> winIter=mPercentKey.iterator();
            while(winIter.hasNext())
            {
                Double winPercent=winIter.next();
                Integer winCount=mWinPercent.get(winPercent);
                ObjectNode winCountN=nf.objectNode();
                winCountN.put("winPercent",winPercent);
                winCountN.put("winCount",winCount);
                winNodes.add(winCountN);
            }
            Iterator<Double> bidIter=mPercentKey.iterator();
            while(bidIter.hasNext())
            {
                Double bidPercent=bidIter.next();
                Integer bidCount=mBidPercent.get(bidPercent);
                ObjectNode bidCountN=nf.objectNode();
                bidCountN.put("bidPercent",bidPercent);
                bidCountN.put("bidCount",bidCount);
                winNodes.add(bidCountN);
            }
            all.put("WinBidPercent",winNodes);
            if(mShwhr!=null && mShwhr.length()>0)
            {
                all.put("shwhr",mShwhr);
            }
            
            return all;
        }catch(Exception ex)
        {
            System.out.println(ex.getMessage());
            return null;
        }
    }
	public static void main(String[] args) throws JsonParseException, IOException 
	{
        byte[] tAuctHist = null;
        byte[] tAuctHist1 = null;
        try
        {
            File tAuctHistFile = new File("/tmp/tauctHist.json");
            File tAuctHistFile1 = new File("/tmp/tauctHist1.json");
            tAuctHist= new byte[(int) tAuctHistFile.length()];
            tAuctHist1= new byte[(int) tAuctHistFile1.length()];
            DataInputStream dis = new DataInputStream(new FileInputStream(tAuctHistFile));
            dis.readFully(tAuctHist);
            dis.close();

            dis = new DataInputStream(new FileInputStream(tAuctHistFile1));
            dis.readFully(tAuctHist1);
            dis.close();
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }

        HashMap<String,StatsCounter> auctMap=fromJson(tAuctHist);
        System.out.println(MinprocParser.getAuctHistAsJsonString(auctMap));
    }
}
