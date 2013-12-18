package mintIntegration;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class handleTest {
	
    private static XMLConfiguration conf = null;
    
    private static Logger log = LoggerFactory.getLogger(handleTest.class);
   
    private static String oid = "Test";
    private static String description = "Test of handle creation";
    private static String url = "http://www.uts.edu.au/";
    
    
    public static void main(String[] args) {

    	String config_file = null;
    	Map<String, String> env = System.getenv();
    	
    	config_file = env.get("RDCMINT_CONFIG");
    	
    	if( config_file == null || config_file.isEmpty() ) {
    		log.error("Set environment variable RDCMINT_CONFIG to config file location.");
    	} else {
    		
    		try {
    			conf = new XMLConfiguration();
    			conf.setDelimiterParsingDisabled(true);
    			conf.load(config_file);
    			
    			Configuration hconf = conf.subset("handles");
    			
    			HandleAdmin handleadmin = new HandleAdmin(hconf);
    			
    			String newhandle = handleadmin.createHandle(oid, description, url);
    			
    			System.out.println("New handle: " + newhandle);
    			
    		} catch ( Exception e ) {
    			log.error("Config error");
    			e.printStackTrace();
    		};
    	
    	}
    } 
    
}
    