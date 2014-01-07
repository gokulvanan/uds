package procstats;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;


// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class GenBidLS {

  public static void main(String[] args) throws IOException {
    // You need a configuration object to tell the client where to connect.
    // When you create a HBaseConfiguration, it reads in whatever you've set
    // into your hbase-site.xml and in hbase-default.xml, as long as these can
    // be found on the CLASSPATH
    Writer out = new BufferedWriter(new OutputStreamWriter(System.out));
    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, "smalluser");
    Scan s = new Scan();
    s.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rtbjson"));
    s.addColumn(Bytes.toBytes("r"), Bytes.toBytes("minprocjson"));
    ResultScanner scanner = table.getScanner(s);
    try 
    {
        for (Result r : scanner) 
        {
            //System.out.println("Row-->"+Bytes.toString(r.getRow()));
            if(r.containsColumn(Bytes.toBytes("r"),Bytes.toBytes("minprocjson")))
            {
                byte [] minprocJson = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("minprocjson"));
                byte [] rtbJson = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("rtbjson"));
                String bidLs=MinprocParser.getBidLandScape(minprocJson,rtbJson);
                Put p = new Put(r.getRow());
                p.add(Bytes.toBytes("m"), Bytes.toBytes("bidls"),Bytes.toBytes(bidLs));
                table.put(p);
                System.out.println(Bytes.toString(r.getRow())+"-->"+bidLs);
            }
        }
    }finally 
    {
      // Make sure you close your scanners when you are done!
      // Thats why we have it inside a try/finally clause
      scanner.close();
    }
    
    /*
    // This instantiates an HTable object that connects you to
    // the "myLittleHBaseTable" table.
    HTable table = new HTable(config, "smalluser");
    HTable newtable = new HTable(config, "smalluser");
    String[] uids={"0010a674-c2db-4b49-be2e-694a64a20da6","0000e26c-6b90-4ac9-92d2-167aa5772b63","00011718-ff5a-4dc3-82a8-efdcaed8e90b","00012bd3-cdf2-4851-9d15-8f447d1e5fc0","00015a6f-c9d9-4be7-a80f-f9c8cf854239"};

    System.out.println("Going into loop");

    // Now, to retrieve the data we just wrote. The values that come back are
    // Result instances. Generally, a Result is an object that will package up
    // the hbase return into the form you find most palatable.
    for (String uid: uids)
    {
        System.out.println("uid="+uid);
        Get g = new Get(Bytes.toBytes(uid));
        Result r = table.get(g);
        byte [] amt = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("amt"));
        byte [] amt_rowcnt = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("amt_rowcnt"));
        byte [] amt_mintime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("amt_mintime"));
        byte [] amt_maxtime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("amt_maxtime"));
        byte [] dco = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("dco"));
        byte [] dco_rowcnt = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("dco_rowcnt"));
        byte [] dco_mintime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("dco_mintime"));
        byte [] dco_maxtime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("dco_maxtime"));
        byte [] rtb = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("rtb"));
        byte [] rtb_rowcnt = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("rtb_rowcnt"));
        byte [] rtb_mintime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("rtb_mintime"));
        byte [] rtb_maxtime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("rtb_maxtime"));
        byte [] minproc = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("minproc"));
        byte [] minproc_rowcnt = r.getValue(Bytes.toBytes("r"),Bytes.toBytes("minproc_rowcnt"));
        byte [] minproc_mintime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("minproc_mintime"));
        byte [] minproc_maxtime= r.getValue(Bytes.toBytes("r"),Bytes.toBytes("minproc_maxtime"));

        Put p = new Put(Bytes.toBytes(uid));

    // To set the value you'd like to update in the row 'myLittleRow', specify
    // the column family, column qualifier, and value of the table cell you'd
    // like to update.  The column family must already exist in your table
    // schema.  The qualifier can be anything.  All must be specified as byte
    // arrays as hbase is all about byte arrays.  Lets pretend the table
    // 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
        if(amt!=null && amt.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("amt"),amt);
        if(amt_rowcnt!=null && amt_rowcnt.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("amt_rowcnt"),amt_rowcnt);
        if(amt_mintime!=null && amt_mintime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("amt_mintime"),amt_mintime);
        if(amt_maxtime!=null && amt_maxtime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("amt_maxtime"),amt_maxtime);

        if(dco!=null && dco.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("dco"),dco);
        if(dco_rowcnt!=null && dco_rowcnt.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("dco_rowcnt"),dco_rowcnt);
        if(dco_mintime!=null && dco_mintime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("dco_mintime"),dco_mintime);
        if(dco_maxtime!=null && dco_maxtime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("dco_maxtime"),dco_maxtime);

        if(rtb!=null && rtb.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("rtb"),rtb);
        if(rtb_rowcnt!=null && rtb_rowcnt.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("rtb_rowcnt"),rtb_rowcnt);
        if(rtb_mintime!=null && rtb_mintime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("rtb_mintime"),rtb_mintime);
        if(rtb_maxtime!=null && rtb_maxtime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("rtb_maxtime"),rtb_maxtime);

        if(minproc!=null && minproc.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("minproc"),minproc);
        if(minproc_rowcnt!=null && minproc_rowcnt.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("minproc_rowcnt"),minproc_rowcnt);
        if(minproc_mintime!=null && minproc_mintime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("minproc_mintime"),minproc_mintime);
        if(minproc_maxtime!=null && minproc_maxtime.length>0)
            p.add(Bytes.toBytes("r"), Bytes.toBytes("minproc_maxtime"),minproc_maxtime);

        newtable.put(p);
        //String valueStr = Bytes.toString(rtb);
        //System.out.println("GET: " + valueStr);
    }
    */

    // Once you've adorned your Put instance with all the updates you want to
    // make, to commit it do the following (The HTable#put method takes the
    // Put instance you've been building and pushes the changes you made into
    // hbase)

    // If we convert the value bytes, we should get back 'Some Value', the
    // value we inserted at this location.
    
    //String valueStr = Bytes.toString(amt);
    //System.out.println("GET: " + valueStr);

    // Sometimes, you won't know the row you're looking for. In this case, you
    // use a Scanner. This will give you cursor-like interface to the contents
    // of the table.  To set up a Scanner, do like you did above making a Put
    // and a Get, create a Scan.  Adorn it with column names, etc.
/*
    Scan s = new Scan();
    s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
    ResultScanner scanner = table.getScanner(s);
    try {
      // Scanners return Result instances.
      // Now, for the actual iteration. One way is to use a while loop like so:
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        // print out the row we found and the columns we were looking for
        System.out.println("Found row: " + rr);
      }

      // The other approach is to use a foreach loop. Scanners are iterable!
      // for (Result rr : scanner) {
      //   System.out.println("Found row: " + rr);
      // }
    } finally {
      // Make sure you close your scanners when you are done!
      // Thats why we have it inside a try/finally clause
      scanner.close();
    }
*/
  }
}
