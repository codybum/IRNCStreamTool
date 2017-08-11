package hbase;

import core.Launcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import sresource.netFlow;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseRecordInsert implements Runnable {

	private Configuration config;
	//public ConcurrentHashMap<String,HTable> thm;
	private HTable table;

	private List<Put> putList;


	public void run()
    {
    	try
		{

    	//thm = new ConcurrentHashMap<String,HTable>();
    	
		System.out.println("Building Hbase Config");

    	config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "bdm00.uky.edu");  // Here we are running zookeeper locally
    	//config.set("hbase.zookeeper.quorum", "10.33.4.72");  // Here we are running zookeeper locally
  	    config.set("hbase.cluster.distributed", "true");  // Here we are running zookeeper locally
        config.set("hbase.rootdir", "hdfs://bdm00.uky.edu:8020/hbase");  // Here we are running zookeeper locally
  	  
  	   //config.set("hbase.rootdir", "hdfs://10.33.4.72:8020/hbase");  // Here we are running zookeeper locally
  	  
  	  
    	//HBaseAdmin admin = new HBaseAdmin(config);
    	  
        //Htable table = new HTable(config, "GENI_QUERYS");
    
    	  //admin.close();
    	  //String.format("%013d", timeStamp)
    	  Launcher.HBASEActive = true;
          
		}
		catch(Exception ex)
		{
			System.out.println("Error Hbase Client : " + ex.toString());
			System.exit(0);
		}
    	
    	//List<Put> putList = new ArrayList<Put>();
    	
        HashMap<String,List<Put>> putMap = null;
        putMap = new HashMap<String,List<Put>>(); //this is to collect inputs
    	putList = new ArrayList<>();

    	try {
			table = new HTable(config, "netflow");
		}
		catch (Exception ex) {
    		System.out.println("Table Create Failed : " + ex.getMessage());
		}
		int count = 0;

		while(Launcher.HBASEActive)
    	{

    		synchronized(Launcher.insertQueue)
    		{

    			while((!Launcher.insertQueue.isEmpty()) && (count < 10000))
    		    {
    				//System.out.println(count);
    		    	try
    		    	{
						InputRecord ir = Launcher.insertQueue.poll();
						if((ir.nf.as_src != null) && (ir.nf.as_dst != null)) {
							if((!ir.nf.as_src.equals("0")) && (!ir.nf.as_dst.equals("0"))) {
								//String key = nf.as_src + "-" + nf.as_dst + "-" + System.currentTimeMillis();
								putList.add(RecordInsert(ir.key, ir.json));
							}
						}
    		    		/*
    		    		QueryRecord qr = Launcher.insertQueue.poll();
    		    		String key = qr.query_id + "-" + String.valueOf(qr.timestamp) + "-" + count;
    		    		Put p = RecordInsert(key,qr.output);
    		    		//System.out.println(key + " " + qr.output);
    		    		if(putMap.containsKey(qr.query_id))
    		    		{
    		    			putMap.get(qr.query_id).add(p);
    		    		}
    		    		else
    		    		{
    		    			//System.out.println("Create # " + qr.query_id);
    		    			List<Put> putList = new ArrayList<Put>();
    		    	        putList.add(p);
    		    			putMap.put(qr.query_id, putList);
    		    		}
    		    		*/
    		    	}
    		    	catch(Exception ex)
    		    	{
    		    		System.out.println("HBClient insert queue: " + ex.toString());
    		    	}
    		    	count++;
    		    }
    		    if(!putList.isEmpty())
    		    {
    		    	try
    		    	{
    		    		System.out.println("Pushing " + count);
    		    		List<Put> plist_tmp = new ArrayList<>();
    		    		plist_tmp.addAll(putList);
						table.put(plist_tmp);
						table.flushCommits();
						putList.clear();
						count = 0;
						/*
    		    		Iterator<?> it = putMap.entrySet().iterator();
    		    		while (it.hasNext()) 
    		    		{
    		    			Map.Entry pairs = (Map.Entry)it.next();
    		    			//System.out.println(pairs.getKey() + " = " + pairs.getValue());
    		    			String query_id = pairs.getKey().toString();
    		    			String tableName = "GENI_QID_" + query_id;
    		    			List<Put> putList = (List<Put>) pairs.getValue();

							System.out.println("processing table: " + tableName + " with " + putList.size() + " records");
							if(thm.contains(tableName))
							{
								thm.get(tableName).put(putList);
								thm.get(tableName).flushCommits();
							}
							else
							{

								HTable table = new HTable(config, tableName);
								table.put(putList);
								table.flushCommits();
								thm.put(tableName, table);
							}

							*/

							/*
    		    			if(Launcher.hbadmin.isTable(query_id))
    		    			{
    		    				System.out.println("processing table: " + tableName + " with " + putList.size() + " records");
    		    				if(thm.contains(tableName))
    		    				{
    		    					thm.get(tableName).put(putList);
    		    					thm.get(tableName).flushCommits();
    		    				}
    		    				else
    		    				{

    		    					HTable table = new HTable(config, tableName);
        		    				table.put(putList);
        		    				table.flushCommits();
        		    				thm.put(tableName, table);				
    		    				}
    		    				
    		    			}
    		    			else
    		    			{
    		    				System.out.println("Not Table: " + tableName + " for " + putList.size() + " records");	    			
    		    			}
    		    			*/
    		    			//it.remove(); // avoids a ConcurrentModificationException
    		    		//}
    		    	}
    		    	catch(Exception ex)
    		    	{
    		    		System.out.println("HBClient insert dequeue: " + ex.toString());
    		    	}

    		    	putMap.clear(); 
    		    }
    		    else
    		    {
    		    	try 
    		    	{
						Thread.sleep(500);
					} 
    		    	catch (InterruptedException e) 
    		    	{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    		    }
    		    
    		}
    	}

    	/*
    	try {
			table.close();
		} catch(Exception ex) {
    		System.out.println("Trying to close table..");
		}
		*/
    	//  admin.close();
    	  
    }

 
private Put RecordInsert(String key, String value) throws IOException
{
        
    	//long timeStamp = System.currentTimeMillis();
    	
        //System.out.println("New Put: query_id:" + query_id + " index:" + index + " value:" + value);
      
        /* To add to a row, use Put. A Put constructor takes the name of the row
        you want to insert into as a byte array. */
        //List<Put> putList = new ArrayList<Put>();
        
        //int problemsize = 50;
        //for(int i = 0; i < problemsize; i++)
        
        //String keyName = nodeName + "-" + String.format("%013d", (timeStamp));
        //String keyName = query_id + "-" + timeStamp + "-" + index;
            
        	//String keyName = "link0-" + String.valueOf(i);
        Put p = new Put(Bytes.toBytes(key));
        /* To set the value you'd like to update in the row 'row1', specify
        the column family, column qualifier, and value of the table cell you'd
        like to update.  The column family must already exist in your table
        schema.  The qualifier can be anything.  */
        //long timeStamp = i * 2000;
        
        //p.add(Bytes.toBytes("GQ"), Bytes.toBytes("rx_bps"),Bytes.toBytes(String.valueOf(generator.nextInt(100000))));
        //p.add(Bytes.toBytes("GQ"), Bytes.toBytes("tx_bps"),Bytes.toBytes(String.valueOf(generator.nextInt(100000))));
        //p.add(Bytes.toBytes("GQ"), Bytes.toBytes("rx_pps"),Bytes.toBytes(String.valueOf(generator.nextInt(100000))));
        //p.add(Bytes.toBytes("GQ"), Bytes.toBytes("query_out"),Bytes.toBytes(value));
		p.add(Bytes.toBytes("json"), Bytes.toBytes("data"),Bytes.toBytes(value));

	return p;
        /* Once you've adorned your Put instance with all the updates you want to
        make, to commit it do the following */
       //putList.add(p);
        //putList.add(p);
        //table.put(putList);
        //putList.clear();
        //table.put(p);
        //table.put(putList);
        /*
          System.out.println("StartTime " + startTime);
          //long endTime = System.currentTimeMillis();
          System.out.println("EndTime " + endTime);
          double etime = ((endTime - startTime)/1000);
          System.out.println("WRITES " + problemsize);
          System.out.println("Elapsed Time " + etime);
          double pps = problemsize/etime;
          System.out.println("Calcs Per Sec " + pps);
          */
        
    }

    
    
}