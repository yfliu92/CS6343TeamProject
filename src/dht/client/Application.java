package dht.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import dht.client.impl.SingleClient;
import dht.common.Configuration;

public class Application {

	public static void main(String[] args) {
		System.out.println("Opening client");
		
		Options options = Configuration.buildOptions();
		CommandLineParser parser = new DefaultParser();
		Configuration config = Configuration.getInstance();
		try {
			CommandLine cmd = parser.parse( options, args);
			config.setConfiguration(cmd);
			
			BaseClient client = null;
			
			// TODO: Add new modes as they are available
			switch(config.getMode()) {
				case "single":
				default:
					client = new SingleClient();
					break;	
			}
			
			for(int i =0; i < 10; i++)
			{
				client.randomWrite();
			}
		} catch (ParseException e) {
			// Print help message if we can't understand command line options
			System.err.println("FATAL: Unable to parse command line: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "DHTServer", options );
			return;
		}

	}

}
