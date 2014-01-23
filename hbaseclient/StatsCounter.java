package procstats;
import java.io.*;
import java.util.*;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;
public class StatsCounter 
{
    //A simple structure to segment the stats according to any criteria
    //The overall stats then is just a sum over all Stats Structure
    public class Stats
    {
        public String mSegment;
        public int mImps,mClicks,mConvs,mBids,mAucs,mAllConvs;
        public Stats(String segment)
        {
            mImps=mClicks=mConvs=mBids=mAucs=mAllConvs=0;
            mSegment=new String(segment);
        }
        public String toString()
        {
            return "segment="+mSegment+"Auctions="+String.valueOf(mAucs)+" Bids="+String.valueOf(mBids)+" Imps="+String.valueOf(mImps)+" Clicks="+String.valueOf(mClicks)+" Conversions="+String.valueOf(mConvs);
        }
    }
    //This is overall impressions, clicks, conversions, bids and auctions
    public int mImps,mClicks,mConvs,mBids,mAucs,mAllConvs;
    HashMap<String,Stats> mStatsMap;
    HashMap<Double,Integer> mWinPercent;
    HashMap<Double,Integer> mBidPercent;
    public StatsCounter()
    {
        mStatsMap=new HashMap<String,Stats>();
        mImps=mClicks=mConvs=mBids=mAucs=mAllConvs=0;
        mWinPercent=new HashMap<Double,Integer>();
        mBidPercent=new HashMap<Double,Integer>();
        mWinPercent.put(new Double(0.0),new Integer(0));    //Count of 0 win percentages
        mWinPercent.put(new Double(0.05),new Integer(0));    //Count of 0 win percentages
        mWinPercent.put(0.10,0);    //Count of <=10 win percentages
        mWinPercent.put(0.25,0);    //Count of <=25 win percentages
        mWinPercent.put(0.75,0);    //Count of <=25 win percentages
        mWinPercent.put(1.0,0);    //Count of <=100 win percentages

        mBidPercent.put(0.0,0);    //Count of 0 bid percentages
        mBidPercent.put(0.05,0);    //Count of <=5 bid percentages
        mBidPercent.put(0.10,0);    //Count of <=10 bid percentages
        mBidPercent.put(0.25,0);    //Count of <=25 bid percentages
        mBidPercent.put(0.75,0);    //Count of <=25 bid percentages
        mBidPercent.put(1.0,0);    //Count of <=100 bid percentages
    }
    public void addWinPercentage(double winPercent,int cnt)
    {
        System.out.println("win%="+winPercent);
        Iterator<Double> winIter=mWinPercent.keySet().iterator();
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
        System.out.println("Bid%="+bidPercent);
        Iterator<Double> bidIter=mBidPercent.keySet().iterator();
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
    public void addBids(String segment,int cnt)
    {
        Stats s=mStatsMap.get(segment);
        if(s==null)
        {
            s=new Stats(segment);
            mStatsMap.put(s.mSegment,s);
        }
        s.mBids+=cnt;
        mBids+=cnt;
    }
    public void addImps(String segment,int cnt)
    {
        Stats s=mStatsMap.get(segment);
        if(s==null)
        {
            s=new Stats(segment);
            mStatsMap.put(s.mSegment,s);
        }
        s.mImps+=cnt;
        mImps+=cnt;
    }
    public void addClicks(String segment,int cnt)
    {
        Stats s=mStatsMap.get(segment);
        if(s==null)
        {
            s=new Stats(segment);
            mStatsMap.put(s.mSegment,s);
        }
        s.mClicks+=cnt;
        mClicks+=cnt;
    }
    public void addAucs(String segment,int cnt)
    {
        Stats s=mStatsMap.get(segment);
        if(s==null)
        {
            s=new Stats(segment);
            mStatsMap.put(s.mSegment,s);
        }
        s.mAucs+=cnt;
        mAucs+=cnt;
    }
    public void addAllConvs(String segment,int cnt)
    {
        Stats s=mStatsMap.get(segment);
        if(s==null)
        {
            s=new Stats(segment);
            mStatsMap.put(s.mSegment,s);
        }
        s.mAllConvs+=cnt;
        mAllConvs+=cnt;
    }
    public void addConvs(String segment,int cnt)
    {
        Stats s=mStatsMap.get(segment);
        if(s==null)
        {
            s=new Stats(segment);
            mStatsMap.put(s.mSegment,s);
        }
        s.mConvs+=cnt;
        mConvs+=cnt;
    }
    public Set<String> getSegments()
    {
        return mStatsMap.keySet();
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
            ArrayNode winNodes=new ArrayNode(nf);
            Iterator<Double> winIter=mWinPercent.keySet().iterator();
            while(winIter.hasNext())
            {
                Double winPercent=winIter.next();
                Integer winCount=mWinPercent.get(winPercent);
                ObjectNode winCountN=nf.objectNode();
                winCountN.put("winPercent",winPercent);
                winCountN.put("winCount",winCount);
                winNodes.add(winCountN);
            }

            Iterator<Double> bidIter=mBidPercent.keySet().iterator();
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
            
            /*
            Iterator<String> keyIter=mStatsMap.keySet().iterator();
            ArrayNode daypart=new ArrayNode(nf);
            while(keyIter.hasNext())
            {
                String key=keyIter.next();
                ObjectNode segmentNode=nf.objectNode();
                Stats s=mStatsMap.get(key);
                segmentNode.put("convs",s.mConvs);
                segmentNode.put("clicks",s.mClicks);
                segmentNode.put("imps",s.mImps);
                segmentNode.put("bids",s.mBids);
                segmentNode.put("aucs",s.mAucs);
                segmentNode.put("daypart",key);
                daypart.add(segmentNode);
                //System.out.println(segmentJsonString);
            }
            all.put("daypart",daypart);
            */
            return all;
        }catch(Exception ex)
        {
            System.out.println(ex.getMessage());
            return null;
        }
    }
    public String toString()
    {
        int clicks,imps,aucs,convs,bids;
        clicks=imps=aucs=convs=bids=0;
        Iterator<String> keyIter=mStatsMap.keySet().iterator();
        while(keyIter.hasNext())
        {
            Stats s=mStatsMap.get(keyIter.next());
            clicks+=s.mClicks;
            imps+=s.mImps;
            convs+=s.mConvs;
            aucs+=s.mAucs;
            bids+=s.mBids;
        }
        return "Auctions="+String.valueOf(aucs)+" Bids="+String.valueOf(bids)+" Imps="+String.valueOf(imps)+" Clicks="+String.valueOf(clicks)+" Conversions="+String.valueOf(convs);
    }
}
