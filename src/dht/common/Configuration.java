package dht.common;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Configuration {
	String host;
	int port;
	String mode; // single | ring | rush | elastic
	
	public static Options buildOptions()
	{
		Options options = new Options();
		Option help = new Option( "help", "print this help message" );
		options.addOption(help);
		
		Option debug = new Option( "debug", "print debugging information" );
		options.addOption(debug);
		
		Option hostname   = Option.builder("h")
				.longOpt( "host" )
				.desc( "bind to provided IP address"  )
			    .hasArg()
			    .argName( "hostname" )
			    .required()
			    .build();
		options.addOption(hostname);
		
		Option port   = Option.builder("p")
				.longOpt( "port" )
				.desc( "listen on provided port number"  )
			    .hasArg()
			    .argName( "port" )
			    .required()
			    .build();
		options.addOption(port);
		
		
		Option mode   = Option.builder("m")
				.longOpt( "mode" )
				.desc( "single | ring | rush | elastic"  )
			    .hasArg()
			    .argName( "mode" )
			    .required()
			    .build();
		options.addOption(mode);
		
		return options;
	}
	
	public Configuration()
	{
		this.host = "";
		this.port = 0;
		this.mode = "";
	}
	
	public void setConfiguration(CommandLine cmd)
	{
		if(cmd.hasOption("h"))
			this.host = cmd.getOptionValue("h");
		else
			System.err.println("Must provide host parameter");
		
		if(cmd.hasOption("p"))
			this.port = Integer.parseInt(cmd.getOptionValue("p"));
		else
			System.err.println("Must provide port parameter");
		
		if(cmd.hasOption("m"))
			this.mode = cmd.getOptionValue("p");
		else
			System.err.println("Must provide mode parameter");
	}
	
	public String getHost()
	{
		return this.host;
	}
	
	public int getPort()
	{
		return this.port;
	}
	
	public String getMode()
	{
		return this.mode;
	}

}
