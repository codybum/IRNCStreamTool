package hbase;


public class QueryRecord {

	  public String query_id;
	  public String output;
	  public long timestamp;
	  
	  
	  public QueryRecord(String query_id, String output, long timestamp)
	  {
		  this.query_id = query_id;
		  this.output = output;
		  this.timestamp = timestamp;
	  }
	  public QueryRecord(String query_id, String output)
	  {
		  this.query_id = query_id;
		  this.output = output;
		  this.timestamp = System.currentTimeMillis();
	  }
	    	  
	}