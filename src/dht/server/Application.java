package dht.server;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import dht.common.Configuration;
import dht.server.impl.SingleServer.SingleServer;

public class Application {
	
	public static void main(String[] args) {
		System.out.println("Opening application");
		
		Options options = Configuration.buildOptions();
		CommandLineParser parser = new DefaultParser();
		Configuration config = new Configuration();
		try {
			CommandLine cmd = parser.parse( options, args);
			config.setConfiguration(cmd);
			
			// TODO: Implement logic for different server implementations based on config.mode
			SingleServer server = new SingleServer(config);
			
			server.buildRouting();
			server.run();
		} catch (ParseException e) {
			// Print help message if we can't understand command line options
			System.err.println("FATAL: Unable to parse command line: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "DHTServer", options );
			return;
		}
	}
}
