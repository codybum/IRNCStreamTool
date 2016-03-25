package core;



import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import sresource.ESPERNetFlow;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static com.espertech.esper.util.DatabaseTypeEnum.String;


public class Launcher {

	
    public static Config conf;
    
    public static ConnectionFactory factory;    
    public static Connection connection;
    
    
	public static void main(String[] args) throws Exception 
	{
		
		//create config
		conf = new Config(checkConfig(args));
		
		//System.out.println(conf.getConfig("general", "amqp_server"));
		String amqp_server = conf.getConfig("amqp", "server");
		String amqp_login = conf.getConfig("amqp", "login");
		String amqp_password = conf.getConfig("amqp", "password");
		String amqp_inexchange = conf.getConfig("amqp", "inexchange");
		String querystring = conf.getConfig("cep", "querystring");
		
		ESPERNetFlow enf = new ESPERNetFlow(amqp_server,amqp_login,amqp_password,amqp_inexchange,querystring);
		Thread et = new Thread(enf);
		et.start();
		
    	
	}
	
	
	public static String checkConfig(String[] args)
	{
		String errorMgs = "StreamReporter\n" +
    			"Usage: java -jar StreamReporter.jar" +
    			" -f <configuration_file>\n";
    			
    	if (args.length != 2)
    	{
    	  System.err.println(errorMgs);
    	  System.err.println("ERROR: Invalid number of arguements.");
      	  System.exit(1);
    	}
    	else if(!args[0].equals("-f"))
    	{
    	  System.err.println(errorMgs);
    	  System.err.println("ERROR: Must specify configuration file.");
      	  System.exit(1);
    	}
    	else
    	{
    		File f = new File(args[1]);
    		if(!f.exists())
    		{
    			System.err.println("The specified configuration file: " + args[1] + " is invalid");
    			System.exit(1);	
    		}
    	}
    return args[1];	
	}
}
