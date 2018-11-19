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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        if(args.length < 2)
        {   
            System.out.println("java GossipRing [ip] [port]");
            System.exit(0);
        }
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
		new GossipRing(ip, port);
	}
	
	/**
	 * Constructor.
	 * This will start the this thread.
	 */
	public GossipRing(String ip, int port) {
        this.ip = ip;
        this.port = port;
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
			teamMembers.add(new RemoteGossipMember(ip, port + 1));
			teamMembers.add(new RemoteGossipMember(ip, port + 2));
			
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
