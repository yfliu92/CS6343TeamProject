package com.google.code.gossip.manager.impl;

import java.util.ArrayList;
import java.io.*;
import java.net.*;

import org.json.JSONArray;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.manager.ActiveGossipThread;
import com.google.code.gossip.manager.GossipManager;
//import com.sun.xml.internal.ws.util.ByteArrayBuffer;


abstract public class SendMembersActiveGossipThread extends ActiveGossipThread {

	public SendMembersActiveGossipThread(GossipManager gossipManager) {
		super(gossipManager);
	}
	
	/**
	 * Performs the sending of the membership list, after we have
	 * incremented our own heartbeat.
	 */
	protected void sendMembershipList(LocalGossipMember me, ArrayList<LocalGossipMember> memberList, int sync_variable) {
		GossipService.debug("Send sendMembershipList() is called." );

		// Increase the heartbeat of myself by 1.
		me.setHeartbeat(me.getHeartbeat() + 1);
		
		synchronized (memberList) {
			try {
                for(LocalGossipMember member:memberList)
				{
                    if (member != _gossipManager.getMyself())
                    {
					    GossipService.debug("start timeouttimer");
                        member.startTimeoutTimer();
                    }
					InetAddress dest = InetAddress.getByName(member.getHost());
					
					// Create a StringBuffer for the JSON message.
					JSONArray jsonArray = new JSONArray();
					GossipService.debug("Sending memberlist to " + dest + ":" + member.getPort());
					GossipService.debug("---------------------");
					
					// First write myself, append the JSON representation of the member to the buffer.
					jsonArray.put(me.toJSONObject());
					GossipService.debug(me);
					
					// Then write the others.
					for (int i=0; i<memberList.size(); i++) {
						LocalGossipMember other = memberList.get(i);
						// Append the JSON representation of the member to the buffer.
						jsonArray.put(other.toJSONObject());
						GossipService.debug(other);
					}
					GossipService.debug("---------------------");
					String sending_message = "Sync:" + sync_variable + ";" + jsonArray.toString();
					GossipService.debug("Sending message: " + sending_message);
                    SocketAddress socketAddress = new InetSocketAddress(dest, member.getPort());
                    Socket socket = new Socket();
                    try{
                        socket.connect(socketAddress, 1000);
			        } catch (IOException e1) {
                        GossipService.debug("Connection Failed: " + dest + ":" + member.getPort());
				        //e1.printStackTrace();
			        }
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter output = new PrintWriter(outputStream, true);
                    output.println(sending_message);
                    output.flush();
					socket.close();
				}
			} catch (IOException e1) {
                GossipService.debug("Connection Failed");
				//e1.printStackTrace();
			}
		}
	}
    protected void sendMessage(LocalGossipMember me, ArrayList<LocalGossipMember> memberList,String text)
    {
        GossipService.debug("Send sendMessage() is called.");
        synchronized (memberList) {
            try {
                for(LocalGossipMember member:memberList)
                {   
                    InetAddress dest = InetAddress.getByName(member.getHost());
                    // Create a StringBuffer for the JSON message.
                    GossipService.debug("Sending message \"" + text + "\" to " + dest + ":" + member.getPort());
                    String sending_message = "Resend:" + 0 + ";" + text;
                    SocketAddress socketAddress = new InetSocketAddress(dest, member.getPort());
                    Socket socket = new Socket();
                    try{
                        socket.connect(socketAddress, 1000);
                    } catch (IOException e1) {
                        GossipService.debug("Connection Failed: " + dest + ":" + member.getPort());
                        //e1.printStackTrace();
                    }   
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter output = new PrintWriter(outputStream, true);
                    output.println(sending_message);
                    output.flush();
                    socket.close();
                }
            } catch (IOException e1) {
                GossipService.debug("Connection Failed");
                //e1.printStackTrace();
            }
        }
    }
}
