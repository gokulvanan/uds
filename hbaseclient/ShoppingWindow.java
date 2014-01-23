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
//
//A simple class to hold shopping window information
public class ShoppingWindow 
{
    public long start,end;
    public int shwhr;
    //A class to create a list of shopping window objects by parsing
    //json string representation of it
    public static List<ShoppingWindow> parseFromJson(byte[] rawData)
    {
        String shwjson=null;
        try
        {
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            shwjson=utf8Decoder.decode(ByteBuffer.wrap(rawData)).toString();
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
            return null;
        }

        List<ShoppingWindow> shwl=new ArrayList<ShoppingWindow>();
        JsonNode rootNode=null;
        //create ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();
        //read JSON like DOM Parser
        try
        {
            rootNode = objectMapper.readTree(shwjson);
        }catch(java.io.IOException e)
        {
            System.out.println(e.getMessage());
            return null;
        }
        Iterator<JsonNode> records=rootNode.getElements();
        while(records.hasNext())
        {
            JsonNode record = records.next();
            if(record.path("start").isMissingNode() ||record.path("end").isMissingNode() ||
                record.path("shwhr").isMissingNode()) 
            {
                continue;
            }
            long start=record.path("start").getLongValue();
            long end=record.path("end").getLongValue();
            int shwhr=record.path("shwhr").getIntValue();
            ShoppingWindow shw=new ShoppingWindow();
            shw.start=start;
            shw.end=end;
            shw.shwhr=shwhr;
            shwl.add(shw);
        }
        return shwl;
    }
    //Get the shopping window segments given a timestamp, list of target window hours and shopping window list
    public static List<String> getShwSegments(long timeStamp,List<Integer> tshwhrl,List<ShoppingWindow> shwl)
    {
        List<String> ret=new ArrayList<String>();
        Iterator<ShoppingWindow> shwiter=shwl.iterator();
        while(shwiter.hasNext())
        {
            ShoppingWindow shw=shwiter.next();
            if(timeStamp>=shw.start && timeStamp<=shw.end )
            {
                Iterator<Integer> tshwiter=tshwhrl.iterator();
                while(tshwiter.hasNext())
                {
                    Integer tshwhr=tshwiter.next();
                    if(shw.shwhr<=tshwhr)
                        ret.add(tshwhr.toString());
                }
                return ret;
            }
        }
        return ret;
    }
}    
