package procstats;
import java.io.*;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.HashSet;
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
    public static List<ShoppingWindow> getShoppingWindow(File f)
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

    public static List<ShoppingWindow> getShoppingWindow(byte[] rawData)
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
        ArrayList<ShoppingEvent> events=new ArrayList<ShoppingEvent>();
        boolean purchased=false;

        String userId=new String();
        while(records.hasNext())
        {
            //System.out.println("Begin of array");
            JsonNode record = records.next();
            //Divide by 1000 to get the unixtimestamp from milliseconds
            long timeStamp=(record.get("timestamp")).getLongValue()/1000; 
            userId=record.get("userId").getValueAsText();

            JsonNode referellURLNode=record.path("referrelURL");
            referrelURL=referellURLNode.getValueAsText();
            if(referrelURL==null || referrelURL.length()<5)
            {
                System.out.println(userId+"\treferrelURL is invalid ");
                continue;
            }
                
            String actionString=record.path("jsonString").getValueAsText();
            //System.out.println("actionString="+actionString);
            JsonNode actionNode=null;
            try
            {
                actionNode = objectMapper.readTree(actionString);
            }catch(Exception e)
            {
                System.out.println(e.getMessage());
                continue;
            }

            String action=actionNode.path("userAction").getValueAsText();
            if(action==null)
            {
                //System.out.println(userId+"\tGarbledUserAction UserAction= "+actionString);
                continue;
            }

            if(action.equalsIgnoreCase("s"))
            {
                ShoppingEvent e=new ShoppingEvent();
                e.url=new String(referrelURL);
                e.ts=timeStamp;
                e.action=new String("s");
                events.add(e);
            }

            /*
            if(referellURLNode.getValueAsText().matches(".*utm_source.*"))
            {
                referrelURL=referellURLNode.getValueAsText();
            }
            */
            if(referrelURL.matches(".*confirm.*"))
            {
                ShoppingEvent e=new ShoppingEvent();
                e.url=new String(referrelURL);
                e.ts=timeStamp;
                e.action=new String("p");
                events.add(e);
                purchased=true;
            }
        }

        if(purchased)
        {
            ArrayList<ShoppingWindow> ret=new ArrayList<ShoppingWindow>();
            Object[] eventA=events.toArray();
            for(int i=0;i<eventA.length;i++)
            {
                ShoppingEvent she=(ShoppingEvent)eventA[i];
                if(she.action.equalsIgnoreCase("p"))  //A purchase event has happened
                {
                    ShoppingEvent pe=(ShoppingEvent)eventA[i];
                    System.out.println(userId+"\t"+pe.ts+"\t"+pe.url);
                    //Seek 1st shooping event 30 minutes before purchase
                    int index=i-1;
                    while(index>0)
                    {
                        ShoppingEvent e=(ShoppingEvent)eventA[index];
                        if(e.action.equalsIgnoreCase("p"))
                            break;
                        else if(e.action.equalsIgnoreCase("s") && (pe.ts-e.ts)>30*60)
                        {
                            ShoppingWindow shw=new ShoppingWindow(); 
                            shw.start=e.ts;
                            shw.end=pe.ts;
                            shw.shwhr=1+(int)(shw.end-shw.start)/3600;
                            ret.add(shw);
                            System.out.println(userId+"\t"+e.ts+"\t"+e.url);
                            System.out.println(userId+"\t"+pe.ts+"\t"+pe.url);
                            break;
                        }else
                        {
                            System.out.println("Ignoring.."+userId+"\t"+pe.ts+"\t"+pe.url);
                            index--;
                        }
                    }
                }
            }
            if(ret.size()>0)
                return ret;
        }
        return null;
    }
    public static String getShoppingWindowAsJson(byte[] jsonData)
    {
        List<ShoppingWindow> shw=getShoppingWindow(jsonData);
        if(shw!=null)
            return convertToJson(shw);
        else
            return null;
    }

    public static String convertToJson(List<ShoppingWindow> ts)
    {
        if(ts==null)
            return null;
        Iterator<ShoppingWindow> tsIter=ts.iterator();
        JsonNodeFactory nf=JsonNodeFactory.instance;
        ArrayNode shwnode=new ArrayNode(nf);
        //The data is guarenteed to come in pairs
        while(tsIter.hasNext())
        {
            ObjectNode data=new ObjectNode(nf);
            ShoppingWindow shw=tsIter.next();
            //Long start=tsIter.next();
            data.put("start",shw.start);
            data.put("end",shw.end);
            data.put("shwhr",shw.shwhr);
            shwnode.add(data);
        }
        return shwnode.toString();
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
