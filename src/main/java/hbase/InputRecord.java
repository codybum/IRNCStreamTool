package hbase;


import sresource.netFlow;


public class InputRecord {

	  public String key;
	  public String source;
	  public long timestamp;
	  public netFlow nf;
	  public String json;

	  public InputRecord(String json, netFlow nf, String source, long timestamp)
	  {
		  this.json = json;
		  this.source = source;
		  this.nf = nf;
		  this.timestamp = timestamp;
		  this.key = nf.as_src + "-" + nf.as_dst + "-" + source  + "-" + timestamp;

	  }
	  public InputRecord(String json, netFlow nf, String source)
	  {
		  this.json = json;
		  this.source = source;
		  this.nf = nf;
		  this.key = nf.as_src + "-" + nf.as_dst + "-" + source  + "-" + System.currentTimeMillis();
	  }
	    	  
	}