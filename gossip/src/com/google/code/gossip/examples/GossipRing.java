package com.google.code.gossip.examples;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LogLevel;
import com.google.code.gossip.RemoteGossipMember;

/**
 * This class is an example of how one could use the gossip service.
 * Here we start multiple gossip clients on this host as specified in the config file.
 * 
 * @author harmenw
 */
public class GossipRing extends Thread {
	/** The number of clients to start. */
	private static final int NUMBER_OF_CLIENTS = 3;
    private int port;
    private String ip;
    private int port1;
    private String ip1;
    private int port2;
    private String ip2;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        if(args.length < 6)
        {   
            System.out.println("java GossipRing [ip] [port] [brother_ip1] [brother_port1] [brother_ip2] [brother_port2]");
            System.exit(0);
        }
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        String ip1 = args[2];
        int port1 = Integer.parseInt(args[3]);
        String ip2 = args[4];
        int port2 = Integer.parseInt(args[5]);
		new GossipRing(ip, port, ip1, port1, ip2, port2);
	}
	
	/**
	 * Constructor.
	 * This will start the this thread.
	 */
	public GossipRing(String ip, int port, String ip1, int port1, String ip2, int port2) {
        this.ip = ip;
        this.port = port;
        this.ip1 = ip1;
        this.port1 = port1;
        this.ip2 = ip2;
        this.port2 = port2;
		start();
	}

	/**
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		try {
			GossipSettings settings = new GossipSettings();
			// Get my ip address.
			GossipMember startupMember = new RemoteGossipMember(ip, port);
			ArrayList<GossipMember> teamMembers = new ArrayList<GossipMember>();
			teamMembers.add(new RemoteGossipMember(ip1, port1));
			teamMembers.add(new RemoteGossipMember(ip2, port2));
			
			// Lets start the gossip clients.
			// Start the clients, waiting cleaning-interval + 1 second between them which will show the dead list handling.
			GossipService gossipService = new GossipService(ip, startupMember.getPort(), LogLevel.DEBUG, teamMembers, settings);
			//GossipService gossipService = new GossipService(myIpAddress, startupMember.getPort(), LogLevel.INFO, teamMembers, settings);
			gossipService.start();
			sleep(settings.getCleanupInterval() + 1000);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
