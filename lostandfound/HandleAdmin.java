package mintIntegration;

/* Handle creation for the RDC/Mint Integration code.
 * 
 * Freely adapted from the HandleTransformer class in ReDBOX/Mint
 *
 */


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.MessageDigest;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.IOUtils;

import net.handle.hdllib.AbstractMessage;
import net.handle.hdllib.AbstractResponse;
import net.handle.hdllib.AddValueRequest;
import net.handle.hdllib.AdminRecord;
import net.handle.hdllib.CreateHandleRequest;
import net.handle.hdllib.Encoder;
import net.handle.hdllib.ErrorResponse;
import net.handle.hdllib.HandleException;
import net.handle.hdllib.HandleResolver;
import net.handle.hdllib.HandleValue;
import net.handle.hdllib.ModifyValueRequest;
import net.handle.hdllib.PublicKeyAuthenticationInfo;
import net.handle.hdllib.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HandleAdmin {

	/** The default web domain - can be overridden in config by 
	 *  handleDomain */
    private static String HANDLE_DEFAULT_DOMAIN = "hdl.handle.net";

    /** Static values used during handle creation */
    private static int ADMIN_INDEX = 100;
    private static int PUBLIC_INDEX = 300;
    private static int URL_INDEX = 3;
    private static String ADMIN_TYPE = "HS_ADMIN";
    private static String DESC_TYPE = "DESC";
    private static String URL_TYPE = "URL";

    private Configuration conf;

    private String privateKeyFile;
    
    /** Handle Resolver */
    private HandleResolver resolver;

    /** Keyed authentication data */
    private PublicKeyAuthenticationInfo authentication;
    
    /** Administrative Record */
    private AdminRecord admin;

    /** The base URL to prepend to Handles */
    private String handleBaseUrl;

    /** Naming Authority */
    private String namingAuthority;

    /** Log */
    private static Logger log = LoggerFactory.getLogger(HandleAdmin.class);

	/**
	 * @param hconf Configuration snippet
	 */
	
	HandleAdmin(Configuration hconf) throws Exception {
		conf = hconf;
		
		init();
		
	}
	
	
	private void init() throws Exception {

		// Do we have a naming authority? No need to evaluate the
        //  complicated stuff if we don't have this
        
		namingAuthority = conf.getString("namingAuthority");
        
		if (namingAuthority == null || namingAuthority.equals("")) {
			throw new Exception("No naming authority specified!");
        }
        
		// The methods below want the data as a byte array
        byte[] prefix = null;
        
        try {
        	prefix = ("0.NA/" + namingAuthority).getBytes("UTF8");
        } catch(Exception ex) {
        	throw new Exception("Error reading naming authority: ", ex);
        }

        // Our basic resolver... processes requests when they are ready
        resolver = new HandleResolver();
        resolver.traceMessages = true;

        // Private key
        PrivateKey privateKey = null;
        try {
            byte[] key = readPrivateKey();
            byte[] passPhrase = readPassPhrase(key);
            key = Util.decrypt(key, passPhrase);
            privateKey = Util.getPrivateKeyFromBytes(key, 0);
        } catch(Exception ex) {
            throw new Exception("Error during key resolution: ", ex);
        }

        // Create our authentication object for this naming authority
        authentication = new PublicKeyAuthenticationInfo(prefix,
        		PUBLIC_INDEX, privateKey);

        // Set up an administrative record, used to stamp admin rights
        //  on new handles. All those 'true' flags give us full access
        admin = new AdminRecord(prefix, PUBLIC_INDEX,
                  	true, true, true, true, true, true,
                    true, true, true, true, true, true);


            // Work out what the base URL for finished Handles will look like
        String handleDomain = conf.getString("publishedDomain");
        if( handleDomain == null || handleDomain.equals("") ) {
        	handleDomain = HANDLE_DEFAULT_DOMAIN;
        }
        handleBaseUrl = "http://"+handleDomain+"/";		
	}

	
	
/**
 * Reads a private key from the configured location and returns
 * in a byte array
 *
 * @return byte[]: The byte data of the private key
 * @throws TransformerException: If the key is not found or inaccessible
 */

private byte[] readPrivateKey() throws Exception {

	// Make sure it's configured
    String keyPath = conf.getString("privateKeyPath");

    if (keyPath == null) {
        throw new Exception("No private key path in config file.");
    }

    // Retrieve it
    try {
        // Access the file
        File file = new File(keyPath);
        if (file == null || !file.exists()) {
            throw new Exception(
                    "The private key file does not exist or cannot" +
                    " be found: '" + keyPath + "'");
        }
        FileInputStream stream = new FileInputStream(file);

        // Stream the file into a byte array
        byte[] response = IOUtils.toByteArray(stream);
        stream.close();
        return response;
    } catch (Exception ex) {
        throw new Exception("Error accessing file: ", ex);
    }
}

/**
 *
 * @param key: The private key to check
 * @return byte[]: The byte data of the pass phrase, possibly null
 * @throws Exception: If 
 */

private byte[] readPassPhrase(byte[] key) throws Exception {
    try {
        if (Util.requiresSecretKey(key)) {
            String password = conf.getString("passPhrase");
            if (password == null || password.equals("") ) {
                log.error("The private key requires a pass phrase and none was provided!");
                throw new Exception("No pass phrase");
            }
            return password.getBytes("UTF8");
        }
    } catch(Exception ex) {
        throw new Exception("Error during key resolution: ", ex);
    }

    // Null is fine if no passphrase is required
    return null;
}







/* createHandle
 * 
 * @params oid			Object unique identifier
 * @params description  Description
 * @paraMs url          URL to point to
 */

	
 public String createHandle(String oid,
            String description, String url) throws Exception {

	 	String suffix;
	 	try {
	 		 	suffix = encryptID(oid);
	 	} catch( Exception e) {
            throw new Exception("Error building the handle suffix");
        }

        // Make sure the suffix is even valid
        String handle = namingAuthority + "/" + suffix;
        byte[] handleBytes = null;
        try {
            handleBytes = handle.getBytes("UTF8");
        } catch (Exception ex) {
            throw new Exception(
                    "Invalid encoding for Suffix: '" + suffix + "'", ex);
        }

        // Prepare the data going to be used inside the handle
        HandleValue adminVal = getAdminHandleValue();
        HandleValue descVal = getDescHandleValue(description);
        if (adminVal == null ) {
        	throw new Exception("Error creating HandleValue: admin");
        }
        if( descVal == null) {
            throw new Exception("Error creating HandleValues: description");
        }

        HandleValue[] values = {adminVal, descVal};
        // Has URL - modify the array
        if (url != null) {
            HandleValue urlVal = getUrlHandleValue(url);
            if (urlVal == null) {
                throw new Exception("Error creating HandleValue: URL");
            }
            values = new HandleValue[] {adminVal, descVal, urlVal};
        }

        // Now prepare the actualy creationg request for sending
        CreateHandleRequest req = new CreateHandleRequest(
                handleBytes, values, authentication);

        // And send
        try {
            log.info("Sending handle create request ...");
            AbstractResponse response = resolver.processRequest(req);
            log.info("... response received.");

            // Success case
            if (response.responseCode != AbstractMessage.RC_SUCCESS) {
                // Failure case... but expected failure
                if (response.responseCode ==
                        AbstractMessage.RC_HANDLE_ALREADY_EXISTS) {
                    log.warn("Handle '{}' already in use", suffix);

                }

                // Failure case... unexpected cause
                if (response instanceof ErrorResponse) {
                    throw new Exception("Error creating handle: " +
                            ((ErrorResponse) response).toString());

                } else {
                    throw new Exception("Unknown error during" +
                            " handle creation. The create API call has" +
                            " failed, but no error response was returned." +
                            " Message: '" +
                            AbstractMessage.getResponseCodeMessage(
                            response.responseCode) + "'");
                }
            }
        } catch (Exception ex) {
            throw new Exception(
                    "Error attempting to create handle:", ex);
        }

        return handleBaseUrl + handle;
    }
 
 	/* encryptID: take a unique ID and anonymise it
 	 * 
 	 * @params id - the unique ID
 	 */
 
 
 	private String encryptID(String id) throws Exception {
 		byte[] idbytes;
 		byte[] hash;
 		
 		try {
 			idbytes = id.getBytes("UTF8");
 			MessageDigest md = MessageDigest.getInstance("MD5");
 			md.update(idbytes);
 			hash = md.digest();
 			String hashStr = new String(hash, "UTF8");
 			return hashStr;
 			 
 		} catch ( Exception e ) {
 			log.error("ID encryption error");
 			throw e;
 		}		
 	}

    
    /** The following methods more or less left unchanged from HandleTransformer */
    
    /**
     * Create a HandleValue object holding a resolvable URL for the handle
     *
     * @param url: The URL to resolve to
     * @return HandleValue: The instantiated value, NULL if errors occurred.
     */
    private HandleValue getUrlHandleValue(String url) {
        byte[] type = null;
        byte[] urlBytes = null;
        try {
            type = URL_TYPE.getBytes("UTF8");
            urlBytes = url.getBytes("UTF8");
        } catch (Exception ex) {
            log.error("Error creating URL handle value: ", ex);
            return null;
        }

        return createHandleValue(URL_INDEX, type, urlBytes);
    }

    /**
     * Create a HandleValue object holding a public description for the handle
     *
     * @param description: The description to use
     * @return HandleValue: The instantiated value, NULL if errors occurred.
     */
    private HandleValue getDescHandleValue(String description) {
        byte[] type = null;
        byte[] descBytes = null;
        try {
            type = DESC_TYPE.getBytes("UTF8");
            descBytes = description.getBytes("UTF8");
        } catch (Exception ex) {
            log.error("Error creating description handle value: ", ex);
            return null;
        }

        return createHandleValue(PUBLIC_INDEX, type, descBytes);
    }

    /**
     * Create a HandleValue object holding admin data to govern the handle
     *
     * @return HandleValue: The instantiated value, NULL if errors occurred.
     */
    private HandleValue getAdminHandleValue() {
        byte[] type = null;
        try {
            type = ADMIN_TYPE.getBytes("UTF8");
        } catch (Exception ex) {
            // This shouldn't occur, given that ADMIN_TYPE is static, but
            //  we'll return a null response if it ever does;
            log.error("Error creating admin handle value: ", ex);
            return null;
        }

        return createHandleValue(ADMIN_INDEX, type,
                Encoder.encodeAdminRecord(admin));
    }

    /**
     * Create a HandleValue using the index, type and value provided.
     *
     * @param index: The index to assign the value
     * @param type: The type of this value
     * @param value: The data to load into this value
     * @return HandleValue: The instantiated value
     */
    private HandleValue createHandleValue(int index, byte[] type, byte[] value) {
        return new HandleValue(index, type, value,
                // You shouldn't need to change any of this,
                //  see handle.net examples for details.
                HandleValue.TTL_TYPE_RELATIVE, 86400,
                now(), null,
                // Security, all rights except 'public write'
                true, true, true, false);
    }

    /**
     * Trivial wrapper to resolve the current time to an integer
     *
     * @return int: The time now as an integer
     */
    private int now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

	
	
	
	

}
