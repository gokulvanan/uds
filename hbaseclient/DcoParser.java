package procstats;
import java.io.*;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.Iterator;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.DecimalFormat;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class DcoParser 
{
/*
 * {"versionid":0,"countryId":null,"responseTimeInMS":2,"userAgent":"\"Mozilla\/5.0(WindowsNT6.2)AppleWebKit\/537.36(KHTML","cityId":3245,"paramString":"25279","jsonString":"{\"userId\":\"00011718-ff5a-4dc3-82a8-efdcaed8e90b\",\"partnerId\":6006,\"time\":1385401630896,\"msgRcvTime\":null,\"businessVertical\":\"r\",\"userAction\":\"c\",\"skuList\":[\"107486\"],\"keyValueList\":null,\"filters\":null,\"params\":{\"id\":\"107486\",\"f\":\"c\",\"t\":\"r\",\"usertype\":\"00\"}}","partnerId":6006,"pageURL":"null","ip":"likeGecko)Chrome\/31.0.1650.57Safari\/537.36\"","timestamp":1385401630896,"tagType":"IMAGE","pixelType":"HTTP_GET","formatId":null,"referrelURL":"http:\/\/www.myntra.com\/Sports-Sandals\/Puma\/Puma-Women-Techno-II-Black-Sports-Sandals\/107486\/buy?searchQuery=mid-season-sale&serp=4&uq=false","userId":"00011718-ff5a-4dc3-82a8-efdcaed8e90b","action":"c","reqType":"t=r&f=c&id=107486&usertype=00","businessVertical":"r","regionId":219}
*/
    public static List<Long> getShoppingWindow(File f)
    {
        byte[] rawData = new byte[(int) f.length()];
        try
        {
            DataInputStream dis = new DataInputStream(new FileInputStream(f));
            dis.readFully(rawData);
            dis.close();
            return getShoppingWindow(rawData);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static List<Long> getShoppingWindow(byte[] rawData)
    {
        try
        {
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            String minProcJsonData=utf8Decoder.decode(ByteBuffer.wrap(rawData)).toString();
            return getShoppingWindow(minProcJsonData);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;

    }
    //public static int getDayPart(long timestamp,int tz=330)
    public static String getDayPart(long timestamp,int tz)
    {
        //long timestamp = (Long)t.get(o);
        //int tz = (Integer)t.get(1);

        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.setTimeInMillis((timestamp + tz*60)*1000);
        Date d = c.getTime();
        DateFormat dateFormatter = new SimpleDateFormat("HH");
        return dateFormatter.format(d);
    }

    //This DS returns a map where key is URID and the value is stats object which contains advid:bidprice,clicks,imps,convs and auctions. 
    public static List<ShoppingWindow> getShoppingWindow(String jsonData)
    {
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
        String referrelURL=new String();
        String confirmURL=new String();
        ArrayDeque<Long> userAction=new ArrayDeque<Long>();
        ArrayDeque<Long> confirmTS=new ArrayDeque<Long>();
        //long referrelURLTS=0;
        //long confirmts=0;
        String userId=new String();
        while(records.hasNext())
        {
            JsonNode record = records.next();
            //Divide by 1000 to get the unixtimestamp from milliseconds
            long timeStamp=(record.get("timestamp")).getLongValue()/1000; 
            userId=record.get("userId").getValueAsText();
            JsonNode referellURLNode=record.path("referrelURL");
            String actionString=record.path("jsonString").getValueAsText();
            JsonNode actionNode=null;
            try
            {
                actionNode = objectMapper.readTree(actionString);
            }catch(java.io.IOException e)
            {
                System.out.println(e.getMessage());
                continue;
            }
            String action=actionNode.path("userAction").getValueAsText();
            if(action.equalsIgnoreCase("s"))
            {
                userAction.add(timeStamp);
            }
            rootNode = objectMapper.readTree();

            //System.out.println(referellURLNode.getValueAsText());
            //if(referrelURLTS==0 && referellURLNode.getValueAsText().matches(".*utm_source.*"))
            if(referellURLNode.getValueAsText().matches(".*utm_source.*"))
            {
                referrelURL=referellURLNode.getValueAsText();
                referrelURLTS.add(timeStamp);
            }else if(referellURLNode.getValueAsText().matches(".*confirm.*"))
            {
                confirmURL=referellURLNode.getValueAsText();
                confirmTS.add(timeStamp);
            }
        }
        ArrayList<Long> ret=new ArrayList<Long>();
        if(referrelURLTS.size()>0 && confirmTS.size()>0)
        {
            
            Object[] confirmTSA=confirmTS.toArray();
            Object[] referrelTSA=referrelURLTS.toArray();
            int i=0,j=0;
            for(;i<confirmTSA.length;i++)
            {
                boolean advancedJ=false;
                while(j<referrelTSA.length && (Long)confirmTSA[i]>(Long)referrelTSA[j])
                {
                    j++;
                    advancedJ=true;
                }
                if(advancedJ) 
                {
                    if((Long)referrelTSA[j-1]<(Long)confirmTSA[i])
                    {
                        ret.add((Long)referrelTSA[j-1]);
                        ret.add((Long)confirmTSA[i]);
                    }
                    System.out.println(userId);
                    System.out.println(referrelTSA[j-1]+"\t"+referrelURL);
                    System.out.println(confirmTSA[i]+"\t"+confirmURL);
                }else if((Long)referrelTSA[j]<(Long)confirmTSA[i]) 
                {
                    ret.add((Long)referrelTSA[j]);
                    ret.add((Long)confirmTSA[i]);
                    j++;
                }
            }
            return ret;
        }
        return null;
    }
    public static String getShoppingWindowAsJson(byte[] jsonData)
    {
        List<Long> shoppingWindow=getShoppingWindow(jsonData);
        if(shoppingWindow!=null)
            return convertToJson(shoppingWindow);
        else
            return null;
    }

    public static String convertToJson(List<Long> ts)
    {
        if(ts==null)
            return null;
        Iterator<Long> tsIter=ts.iterator();
        JsonNodeFactory nf=JsonNodeFactory.instance;
        ArrayNode shw=new ArrayNode(nf);
        //The data is guarenteed to come in pairs
        while(tsIter.hasNext())
        {
            ObjectNode data=new ObjectNode(nf);
            Long start=tsIter.next();
            data.put("start",start);
            Long end=tsIter.next();
            data.put("end",end);
            shw.add(data);
            Integer shwh=1+(int)(end-start)/3600; //Add one 
            data.put("shwhr",shwh);
        }
        return shw.toString();
    }
 
	public static void main(String[] args) throws JsonParseException, IOException 
	{
        byte[] dco = null;
        try
        {
            File dcoFile = new File("/tmp/dco.json");
            dco = new byte[(int) dcoFile.length()];

            DataInputStream dis = new DataInputStream(new FileInputStream(dcoFile));
            dis.readFully(dco);
            dis.close();

        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        System.out.println(getShoppingWindowAsJson(dco));
        System.out.println("Finished Parsing");
    }
}
