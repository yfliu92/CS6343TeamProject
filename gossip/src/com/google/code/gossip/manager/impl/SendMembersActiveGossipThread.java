package com.google.code.gossip.manager.impl;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;

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
					
					// Write the objects to a byte array.
					byte[] json_bytes = jsonArray.toString().getBytes();
					
					int packet_length = json_bytes.length;
					
					if (packet_length < GossipManager.MAX_PACKET_SIZE) {
						
						// Convert the packet length to the byte representation of the int.
						byte[] length_bytes = new byte[4];
                        length_bytes[0] = (byte)( sync_variable );
						length_bytes[1] =(byte)( (packet_length << 8) >> 24 );
						length_bytes[2] =(byte)( (packet_length << 16) >> 24 );
						length_bytes[3] =(byte)( (packet_length << 24) >> 24 );
						
						GossipService.debug("Sending message ("+packet_length+" bytes): " + jsonArray.toString());
						
						//ByteArrayBuffer byteBuffer = new ByteArrayBuffer();
						// Write the first 4 bytes with the length of the rest of the packet.
						//byteBuffer.write(length_bytes);
						// Write the json data.
						//byteBuffer.write(json_bytes);
						
						//byte[] buf = byteBuffer.getRawData();
						byte[] buf = new byte[length_bytes.length + json_bytes.length];
                        System.arraycopy(length_bytes, 0, buf, 0, length_bytes.length);  
                        System.arraycopy(json_bytes, 0, buf, length_bytes.length, json_bytes.length);  
						
						DatagramSocket socket = new DatagramSocket();
						DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest, member.getPort());
						socket.send(datagramPacket);
						socket.close();
					} else {
						GossipService.error("The length of the to be send message is too large (" + packet_length + " > " + GossipManager.MAX_PACKET_SIZE + ").");
					}
				}
			} catch (IOException e1) {
				e1.printStackTrace();
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
                    byte[] text_bytes = text.getBytes();
                    GossipService.debug("Sending message \"" + text + "\" to " + dest + ":" + member.getPort());
					byte[] buf = new byte[4 + text_bytes.length];
					byte[] length_bytes = new byte[4];
                    length_bytes[0] =(byte)(  text_bytes.length >> 24 );
                    length_bytes[1] =(byte)( (text_bytes.length << 8) >> 24 );
                    length_bytes[2] =(byte)( (text_bytes.length << 16) >> 24 );
                    length_bytes[3] =(byte)( (text_bytes.length << 24) >> 24 );
                    System.arraycopy(length_bytes, 0, buf, 0, 4);
                    System.arraycopy(text_bytes, 0, buf, 4, text_bytes.length);
                    DatagramSocket socket = new DatagramSocket();
                    DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest, member.getPort());
                    socket.send(datagramPacket);
                    socket.close();
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}
