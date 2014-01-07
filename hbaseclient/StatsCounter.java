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
        public int mImps,mClicks,mConvs,mBids,mAucs;
        public Stats(String segment)
        {
            mImps=mClicks=mConvs=mBids=mAucs=0;
            mSegment=new String(segment);
        }
        public String toString()
        {
            return "segment="+mSegment+"Auctions="+String.valueOf(mAucs)+" Bids="+String.valueOf(mBids)+" Imps="+String.valueOf(mImps)+" Clicks="+String.valueOf(mClicks)+" Conversions="+String.valueOf(mConvs);
        }
    }
    //This is overall impressions, clicks, conversions, bids and auctions
    public int mImps,mClicks,mConvs,mBids,mAucs;
    HashMap<String,Stats> mStatsMap;
    public StatsCounter()
    {
        mStatsMap=new HashMap<String,Stats>();
        mImps=mClicks=mConvs=mBids=mAucs=0;
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
            all.put("convs",mConvs);
            all.put("clicks",mClicks);
            all.put("imps",mImps);
            all.put("bids",mBids);
            all.put("aucs",mAucs);
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
