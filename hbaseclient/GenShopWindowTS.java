package procstats;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;


// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class GenShopWindowTS {

  public static void main(String[] args) throws IOException 
  {
    // You need a configuration object to tell the client where to connect.
    // When you create a HBaseConfiguration, it reads in whatever you've set
    // into your hbase-site.xml and in hbase-default.xml, as long as these can
    // be found on the CLASSPATH
    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, "user");
    //Get overall stats object if it exists
    /*
    Get g = new Get(Bytes.toBytes(overallRow));
    Result r = table.get(g);
    */

    Scan s = new Scan();
    s.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dcojson"));
    s.addColumn(Bytes.toBytes("m"), Bytes.toBytes("tshw"));
    ResultScanner scanner = table.getScanner(s);
    try 
    {
        int rownum=0;
        HashMap<String,StatsCounter> overallBidLs=null;
        for (Result r : scanner) 
        {
            //System.out.println("Row-->"+Bytes.toString(r.getRow()));
            //Iterate over all rows which have dco entries and generate shopping window ts 
            //generated
            if(r.containsColumn(Bytes.toBytes("r"),Bytes.toBytes("dcojson")))
            {
                byte [] dcojson = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("dcojson"));
                if(dcojson!=null)
                {
                    System.out.println("Processing "+ Bytes.toString(r.getRow()));
                    String shw=DcoParser.getShoppingWindowAsJson(dcojson);
                    if(shw!=null && !shw.isEmpty())
                    {
                        Put p = new Put(r.getRow());
                        p.add(Bytes.toBytes("m"), Bytes.toBytes("tshw"),Bytes.toBytes(shw));
                        table.put(p);
                        rownum++;
                        System.out.println(Bytes.toString(r.getRow())+" : "+ shw);
                    }else
                    {
                        System.out.println(Bytes.toString(r.getRow())+" shw is empty");
                        if(r.containsColumn(Bytes.toBytes("m"),Bytes.toBytes("tshw")))
                        {
                            Delete d=new Delete(r.getRow());
                            d.deleteColumn(Bytes.toBytes("m"), Bytes.toBytes("tshw"));
                            table.delete(d);
                            System.out.println("Deleting tsw for "+Bytes.toString(r.getRow()));
                        }
                    }
                }else
                {
                    System.out.println(Bytes.toString(r.getRow())+" dcojson is null");
                }
            }
        }
        System.out.println("Generated "+rownum+" shoppingWindow");
    }finally 
    {
      // Make sure you close your scanners when you are done!
      // Thats why we have it inside a try/finally clause
      scanner.close();
    }
  }
}
