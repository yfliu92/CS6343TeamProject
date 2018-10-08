package dht.common;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Configuration {
	private static Configuration instance = null;
	String host;
	int port;
	String mode; // single | ring | rush | elastic
	String nodeid;
	int N;
	boolean sendKill;
	
	public static Options buildOptions()
	{
		Options options = new Options();
		Option help = new Option( "help", "print this help message" );
		options.addOption(help);
		
		Option debug = new Option( "debug", "print debugging information" );
		options.addOption(debug);
		
		Option kill = new Option( "kill", "Client sends kill command at the end of run" );
		options.addOption(kill);
		
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
		
		Option nodeid = Option.builder("i")
				.longOpt( "node-id" )
				.desc( "Set the server's node ID"  )
			    .hasArg()
			    .argName( "nodeid" )
			    .build();
		options.addOption(nodeid);
		
		return options;
	}
	
	private Configuration()
	{
		this.host = "";
		this.port = 0;
		this.mode = "";
		this.N = 1 << 14;
		this.nodeid = "D101";
		this.sendKill = false;
	}
	
	public static Configuration getInstance() {
		if (instance == null)
			instance = new Configuration();
		return instance;
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
			this.mode = cmd.getOptionValue("m");
		else
			System.err.println("Must provide mode parameter");
		
		if(cmd.hasOption("i"))
			this.nodeid = cmd.getOptionValue("i");
		
		if(cmd.hasOption("kill"))
			this.sendKill = true;
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
	
	public String getNodeId()
	{
		return this.nodeid;
	}
	
	public boolean sendKill()
	{
		return this.sendKill;
	}

}
