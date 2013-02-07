package mintIntegration;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Feed {

    private static Logger log = LoggerFactory.getLogger(Feed.class);

	private String dir;
	private String name;
	private String sql;
	private String file;
	private Configuration conf;
	private int primary_key_column;
	private int fors_column;
	private int max_fors;
	private int n_infields;
	private int n_outfields;
	private String fors_prefix;
	private boolean has_fors;
	private boolean trace;
	private HashMap<String, HashMap<String, String>> records;
	
	private ArrayList<String> infields;
	private ArrayList<String> outfields;

	
	/* FIXME: this could do with some exception handling against 
	 * invalid config files
	 */
	
	Feed(String working_dir, Configuration qconf) {
		dir = working_dir;
		conf = qconf;
		name = conf.getString("[@name]");
		sql = conf.getString("sql");
		file = conf.getString("outfields(0)[@file]");
		String trace_v = conf.getString("[@trace]");
		if( trace_v != null ) {
			trace = true;
		} else {
			trace = false;
		}
		loadFields();
	}
	
	
	private void loadFields() {
		
    	n_infields = conf.getList("infields.field[@name]").size();
    	
    	infields = (ArrayList<String>)new ArrayList();
    	outfields = (ArrayList<String>)new ArrayList();
    	
    	for( int i = 0; i < n_infields; i++ ) {
    		String prefix = "infields.field(" + i + ")";
    		infields.add(i, conf.getString(prefix + "[@name]"));
    		if ( conf.getString(prefix + "[@unique_ID]") != null ) {
    			primary_key_column = i;
    		}
    		if ( conf.getString(prefix + "[@fors]") != null ) {
    			fors_column = i;
    			max_fors = conf.getInt(prefix + "[@fors]");
    			has_fors = true;
				fors_prefix = infields.get(i);
    		}
    	}
    	
    	// Note: some queries have multiple lists of outfields.
    	// This assumes that the first one is the 'raw' set for
    	// the initial query.
    	
    	n_outfields = conf.getList("outfields(0).field[@name]").size();
    	
    	for( int i = 0; i < n_outfields; i++ ) {
    		String prefix = "outfields.field(" + i + ")";
    		outfields.add(i, conf.getString(prefix + "[@name]"));
    	}
    	

	}


	
    public void runQuery(Connection con) {
    	
    	Statement stmt = null;
    	ResultSet rset = null;
    	
    	records = (HashMap<String, HashMap<String, String>>)new HashMap();

    	log.debug("Running query: " + name);

    	try {
    		stmt = con.createStatement();
	    
    		log.debug("SQL: " + sql);
    		rset = stmt.executeQuery(sql);

    		ResultSetMetaData rsmd = rset.getMetaData();
    		int cols = rsmd.getColumnCount();

       		while( rset.next() ) {
    			String[] line = new String[cols];
    			for( int i = 0; i < cols; i++ ) {
    				line[i] = StringUtils.replace(rset.getString(i + 1), "\n", "<br />");
    				line[i] = StringUtils.replace(line[i], "\r", "");
    				if( line[i] == null ) {
    					line[i] = "";
    				}
    			}
    			String id = StringUtils.trim(line[primary_key_column]);
    			if( trace ) {
    				log.debug("ID = " + id);
    				log.debug("Row: " + StringUtils.join(line, ','));
    			}
    			if( records.containsKey(id) ) {
    				HashMap<String, String> record = records.get(id);
    				if( has_fors ) {
    					setFOR(record, line[fors_column]);
    				} else {
    					log.debug("Warning: multiple records with ID='" + id + "'");
    				} 
    			} else {
    				HashMap<String, String> record = (HashMap<String, String>)new HashMap();
    				for( int i = 0; i < n_infields; i++ ) {
    					// have to explicitly trim whitespace, because the CSV writer isn't
    					// doing it for me
    					record.put(infields.get(i), StringUtils.trim(line[i]));
    				}
        			if( trace ) {
        				log.debug("Storing record with id = '" + id + "'");
        				
        			}
    				records.put(id, record);
    			}    			
    		}
    		
    		rset.close();
    		stmt.close();
    		
    		log.debug("Got " + records.size() + " records");
    		
    	} catch ( Exception e ) {
    		e.printStackTrace();
    	}
    }
    
    
    private void setFOR(HashMap<String, String> record, String FOR) {
    	boolean setfor = false;
    	for( int j = 0; j < max_fors && !setfor; j++ ) {
    		String field = fors_prefix + "_" + (j + 1);
    		if( ! record.containsKey(field) ) {
    			record.put(field, StringUtils.trim(FOR));
    			setfor = true;
    		}
    	}
    	if( !setfor ) {
    		String id = record.get(infields.get(primary_key_column));
    		log.warn("Warning: more than " + max_fors + " FOR codes on ID=" + id);
    	}
    }
    

    public void printCSV() {
    	FileWriter fw = null;
    	CSVPrinter csv = null;

    	CSVStrategy csv_settings = (CSVStrategy)CSVStrategy.DEFAULT_STRATEGY.clone();
    	
    	csv_settings.setIgnoreTrailingWhitespaces(true);
    	
    	String path = dir + '/' + file;
    	
    	log.debug("Writing CSV to " + path);
    	
    	try {
    		fw = new FileWriter(path);
    	} catch(Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    	
    	try {
    		csv = new CSVPrinter(fw, csv_settings);
    	} catch(Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    	
    	String[] csvheader = new String[n_outfields];
    	for( int i = 0; i < n_outfields; i++ ) {
    		csvheader[i] = outfields.get(i);
    	}
 
		try {
			csv.println(csvheader);
		} catch( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
    	
    	
    	for( Map.Entry<String, HashMap<String, String>> item: records.entrySet()  ) {
    		String id = item.getKey();
    		HashMap<String, String> record = item.getValue();
    		
    		String[] csvline = new String[n_outfields];
    		for ( int i = 0; i < n_outfields; i++ ) {
    			String field = outfields.get(i);
    			if( record.containsKey(field) ) { 
    				csvline[i] = record.get(field);
    			} else {
    				csvline[i] = "";
    			}
    		}
    		if( trace ) {
    			log.debug("Writing CSV, ID = '" + id + "': " + StringUtils.join(csvline, ','));
    		}
    		try {
    			csv.println(csvline);
    		} catch( Exception e ) {
    			log.error("Failed to print CSV line");
    			e.printStackTrace();
    			System.exit(1);
    		}
    	}
    	try{
    		fw.close();
    	} catch(Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    }
    
    
    
}
