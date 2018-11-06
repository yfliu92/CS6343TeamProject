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
public class GossipExample3 extends Thread {
	/** The number of clients to start. */
	private static final int NUMBER_OF_CLIENTS = 2;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new GossipExample3();
	}
	
	/**
	 * Constructor.
	 * This will start the this thread.
	 */
	public GossipExample3() {
		start();
	}

	/**
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		try {
			GossipSettings settings = new GossipSettings();
			
			// Get my ip address.
			String myIpAddress = InetAddress.getLocalHost().getHostAddress();
			
			// Create the gossip members and put them in a list and give them a port number starting with 2000.
			GossipMember startupMember = new RemoteGossipMember(myIpAddress, 4001);
			ArrayList<GossipMember> teamMembers = new ArrayList<GossipMember>();
			teamMembers.add(new RemoteGossipMember(myIpAddress, 4000));
			teamMembers.add(new RemoteGossipMember(myIpAddress, 4002));
			
			// Lets start the gossip clients.
			//Start the clients, waiting cleaning-interval + 1 second between them which will show the dead list handling.
			GossipService gossipService = new GossipService(myIpAddress, startupMember.getPort(), LogLevel.DEBUG, teamMembers, settings);
			gossipService.start();
			sleep(settings.getCleanupInterval() + 1000);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
