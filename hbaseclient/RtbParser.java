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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class RtbParser 
{
    public static HashMap getAdvIdAndBids(File f,HashMap<String,URIDCounter> procMap)
    {
        byte[] rawData = new byte[(int) f.length()];
        try
        {
            DataInputStream dis = new DataInputStream(new FileInputStream(f));
            dis.readFully(rawData);
            dis.close();
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            String rtbJsonData=utf8Decoder.decode(ByteBuffer.wrap(rawData)).toString();
            return getAdvIdAndBids(rtbJsonData,procMap);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static HashMap getAdvIdAndBids(byte[] rtbData,HashMap<String,URIDCounter> procMap)
    {
        try
        {
            CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
            String rtbJsonData=utf8Decoder.decode(ByteBuffer.wrap(rtbData)).toString();
            return getAdvIdAndBids(rtbJsonData,procMap);
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        return null;
    }
    //This DS returns a map where key is advid:bidprice and the value is another map with URIDs and a counter
    public static HashMap getAdvIdAndBids(String jsonData,HashMap<String,URIDCounter> procMap)
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
        while(records.hasNext())
        {
            JsonNode record = records.next();
            JsonNode urIdNode=record.path("uniqueResponseId");
            String urId=urIdNode.getValueAsText();
            if(urId.length()<=3)
            {
                urId=new String("NAC");
            }
            String advId= record.path("advertiserId").getValueAsText();
            int clicks= record.path("clicks").getIntValue();
            int imps= record.path("impressions").getIntValue();
            int conversions= record.path("conversions").getIntValue();
            DecimalFormat df = new DecimalFormat("0.0000000");
            String winningBidPrice=advId+":"+df.format(record.path("winningBidPrice").getDoubleValue()/1000);
            URIDCounter uc=procMap.get(urId);
            if(uc!=null)
            {
                uc.setWinningBidPrice(winningBidPrice);
                uc.addClicks(clicks);
                uc.addImps(imps);
                uc.addConvs(clicks);
            }
        }
        return procMap;
    }
}
