package core;

import java.io.File;
import java.io.IOException;

import org.ini4j.InvalidFileFormatException;
import org.ini4j.Wini;

public class Config {

	//private HierarchicalINIConfiguration iniConfObj;
	private Wini ini;
	
	public Config(String configFile) 
	{
	    //iniConfObj = new HierarchicalINIConfiguration(configFile);
	    //iniConfObj.setAutoSave(true);
		try {
			ini = new Wini(new File(configFile));
		} catch (InvalidFileFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getConfig(String group, String setting)
	{
		//SubnodeConfiguration sObj = iniConfObj.getSection("general");
		//String dir = ini.get("happy", "homeDir");
		//return sObj.getString("amqp_server");
		return ini.get(group,setting);
	}
	
	
	
}