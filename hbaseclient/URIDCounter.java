package procstats;
import java.io.*;
import java.util.*;
public class URIDCounter 
{
    public String mUrid;
    public String mBidPrice,mWinningBidPrice;
    public int mAucs,mImps,mClicks,mBids,mConvs;
    public int mAllConvs;
    public long mTimeStamp;
    public double mWinPercent,mBidPercent;
    public List<String> mSegments;

    public URIDCounter(String urid,String bidString,long timeStamp)
    {
        mUrid=new String(urid);
        mBidPrice=new String(bidString);
        mAucs=mImps=mClicks=mBids=mConvs=mAllConvs=0;
        mWinningBidPrice=null;
        mTimeStamp=timeStamp;
        mSegments=null;
    }

    public void setSegments(List<String> segments)
    {
        mSegments=new ArrayList<String>();
        mSegments.addAll(segments);
    }
    public List<String> getSegments()
    {
        return mSegments;
    } 
    public void addAllConvs(int cnt)
    {
        mAllConvs+=cnt;
    }
    public double getBidPercent()
    {
        if(mBids>0 && mAucs>0)
        {
            mBidPercent=(float)mBids/mAucs;
        }else
        {
            mBidPercent=0;
        }
        return mBidPercent;
    }
    public double getWinPercent()
    {
        if(mBids>0 && mImps>0)
        {
            mWinPercent=(float)mImps/mBids;
        }else
        {
            mWinPercent=0;
        }
        return mWinPercent;
    }
    
    public void addImps(int cnt)
    {
        mImps+=cnt;
    }
    public void addClicks(int cnt)
    {
        mClicks+=cnt;
    }
    public void addBids(int cnt)
    {
        mBids+=cnt;
    }
    public void addAucs(int cnt)
    {
        mAucs+=cnt;
    }
    public void addConvs(int cnt)
    {
        mConvs+=cnt;
    }
    public void setWinningBidPrice(String winningBidPrice)
    {
        mWinningBidPrice=new String(winningBidPrice);
    }
    public String  getWinningBidPrice()
    {
        return mWinningBidPrice;
    }
    public String  getAuctionPrice()
    {
        return getBidPrice();
    }
    public String  getBidPrice()
    {
        return mBidPrice;
    }
    public String getUrid()
    {
        return mUrid;
    }
    public int getImps()
    {
        return mImps;
    }
    public int  getClicks()
    {
        return mClicks;
    }
    public int  getBids()
    {
        return mBids;
    }
    public int  getAucs()
    {
        return mAucs;
    }
    public int  getConvs()
    {
        return mConvs;
    }
    public int  getAllConvs()
    {
        return mAllConvs;
    }
    public int getDayPart()
    {
        return (int)(mTimeStamp%86400)/3600;
    }
    public String toString()
    {
        StringBuilder sb=new StringBuilder();
        sb.append("bid="+mBidPrice+" Auctions="+String.valueOf(mAucs)+" Bids="+String.valueOf(mBids)+" Imps="+String.valueOf(mImps)+" Clicks="+String.valueOf(mClicks)+" Conversions="+String.valueOf(mConvs));
        if(mWinningBidPrice!=null && !mWinningBidPrice.isEmpty())
        {
           sb.append(" WinningBidPrice="+mWinningBidPrice); 
        }
        return sb.toString();
    }
}
