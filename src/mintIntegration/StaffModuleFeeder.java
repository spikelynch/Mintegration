package mintIntegration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class StaffModuleFeeder {
	
    static XMLConfiguration conf = null;
    static Connection con = null;
    
    public static void main(String[] args) {

    	String config_file = null;
    	Map<String, String> env = System.getenv();
    	String working_dir = null;
    	
    	config_file = env.get("RDCMINT_CONFIG");
    	
    	if( config_file == null || config_file.isEmpty() ) {
    		System.out.println("Set environment variable RDCMINT_CONFIG to config file location.\n");
    	} else {
    		
    		try {
    			conf = new XMLConfiguration();
    			conf.setDelimiterParsingDisabled(true);
    			conf.load(config_file);

    			dbConnect();
    			
    			Configuration locations = conf.subset("locations");
    			working_dir = locations.getString("working");
           		Configuration queries = conf.subset("queries");

        		int i = 0;
        	
        		while( queries.getString("query(" + i + ")[@name]") != null ) {
        			Configuration qcon = queries.subset("query(" + i + ")");
        			String name = queries.getString("query(" + i + ")[@name]");
        			i++;
        			System.out.println(name);
        			Feed feed = new Feed(working_dir, qcon);
        			feed.runQuery(con);
        			feed.printCSV();
        		}
        		System.out.println("Done.");
    		} catch( ConfigurationException e ) {
    			e.printStackTrace();
    		};
    	
    	}
    } 
    
    
    
    
    
    static void dbConnect() {
    	
    	String type = conf.getString("connection.type");
    	String server = conf.getString("connection.server");
    	String port = conf.getString("connection.port");
    	String database = conf.getString("connection.database");
    	String domain = conf.getString("connection.domain");
    	String user = conf.getString("connection.user");
    	String password = conf.getString("connection.password");
    	String dbi = "jdbc:jtds:" + type + "://" + server + ":" + port + "/" + database + ";domain=" + domain; 

    	System.out.println("DBI: " + dbi);
    	
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            con = DriverManager.getConnection(dbi, user, password);
            System.out.println("Connected to database");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
