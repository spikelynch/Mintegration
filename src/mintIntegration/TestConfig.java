package mintIntegration;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;


public class TestConfig {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		XMLConfiguration xml = null;
		
		try {
			xml = new XMLConfiguration();
			xml.setDelimiterParsingDisabled(true);
			xml.load("test.xml");
			String foo = xml.getString("bar.foo(2).bang");
			System.out.println("foo = '" + foo + "'");

		} catch ( ConfigurationException e ) {
			e.printStackTrace();
		}

	}

}
