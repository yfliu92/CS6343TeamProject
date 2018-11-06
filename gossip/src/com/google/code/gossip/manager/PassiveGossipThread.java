package com.google.code.gossip.manager;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.String;
import java.net.InetAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.RemoteGossipMember;

/**
 * [The passive thread: reply to incoming gossip request.]
 * This class handles the passive cycle, where this client
 * has received an incoming message.  For now, this message
 * is always the membership list, but if you choose to gossip
 * additional information, you will need some logic to determine
 * the incoming message.
 */
abstract public class PassiveGossipThread implements Runnable {
	
	/** The socket used for the passive thread of the gossip service. */
	private DatagramSocket _server;
	
	private GossipManager _gossipManager;

	private AtomicBoolean _keepRunning;

	public PassiveGossipThread(GossipManager gossipManager) {
		_gossipManager = gossipManager;
		
		// Start the service on the given port number.
		try {
			_server = new DatagramSocket(_gossipManager.getMyself().getPort());
			
			// The server successfully started on the current port.
			GossipService.info("Gossip service successfully initialized on port " + _gossipManager.getMyself().getPort());
			GossipService.debug("I am " + _gossipManager.getMyself());
		} catch (SocketException ex) {
			// The port is probably already in use.
			_server = null;
			// Let's communicate this to the user.
			GossipService.error("Error while starting the gossip service on port " + _gossipManager.getMyself().getPort() + ": " + ex.getMessage());
			System.exit(-1);
		}
		
		_keepRunning = new AtomicBoolean(true);
	}

	@Override
	public void run() {
		while(_keepRunning.get()) {
			try {
				// Create a byte array with the size of the buffer.
				byte[] buf = new byte[_server.getReceiveBufferSize()];
				DatagramPacket p = new DatagramPacket(buf, buf.length);
				_server.receive(p);
				GossipService.debug("A message has been received from " + p.getAddress() + ":" + p.getPort() + ".");
				
                int sync_variable = buf[0];
		        int packet_length = 0;
		        for (int i = 1; i < 4; i++) {
		            int shift = (4 - 1 - i) * 8;
		            packet_length += (buf[i] & 0x000000FF) << shift;
		        }
		        
		        // Check whether the package is smaller than the maximal packet length.
		        // A package larger than this would not be possible to be send from a GossipService,
		        // since this is check before sending the message.
		        // This could normally only occur when the list of members is very big,
		        // or when the packet is misformed, and the first 4 bytes is not the right in anymore.
		        // For this reason we regards the message.
		        if (packet_length <= GossipManager.MAX_PACKET_SIZE) {
		        
			        byte[] json_bytes = new byte[packet_length];
			        for (int i=0; i<packet_length; i++) {
			        	json_bytes[i] = buf[i+4];
			        }

					// Extract the members out of the packet
					String receivedMessage = new String(json_bytes);
					GossipService.debug("Received message (" + packet_length + " bytes): " + receivedMessage);
					
					try {
						
						ArrayList<GossipMember> remoteGossipMembers = new ArrayList<GossipMember>();
						
						RemoteGossipMember senderMember = null;
						if(receivedMessage.startsWith("[{"))
                        {
						GossipService.debug("Received member list:");
						// Convert the received JSON message to a JSON array.
						JSONArray jsonArray = new JSONArray(receivedMessage);
						// The JSON array should contain all members.
						// Let's iterate over them.
						for (int i = 0; i < jsonArray.length(); i++) {
							JSONObject memberJSONObject = jsonArray.getJSONObject(i);
							// Now the array should contain 3 objects (hostname, port and heartbeat).
							if (memberJSONObject.length() == 3) {
								// Ok, now let's create the member object.
								RemoteGossipMember member = new RemoteGossipMember(memberJSONObject.getString(GossipMember.JSON_HOST), memberJSONObject.getInt(GossipMember.JSON_PORT), memberJSONObject.getInt(GossipMember.JSON_HEARTBEAT));
								GossipService.debug(member.toString());
								
								// This is the first member found, so this should be the member who is communicating with me.
								if (i == 0) {
									senderMember = member;
								}
								remoteGossipMembers.add(member);
							} else {
								GossipService.error("The received member object does not contain 3 objects:\n" + memberJSONObject.toString());
							}
							
						}
						// Merge our list with the one we just received
						mergeLists(_gossipManager, senderMember, remoteGossipMembers);
                        // Gossip spread among all servers
						GossipService.debug("sync_variable:" + sync_variable + " _gossipManager.sync_variable:" + _gossipManager.sync_variable);
                        if(sync_variable > _gossipManager.sync_variable)
                        {
                            _gossipManager.sync_variable = sync_variable;
                            reSendMembershipList(_gossipManager.getMyself(), senderMember, _gossipManager.getMemberList(), _gossipManager.sync_variable);
                        }
						}
                        else
                        {
                            GossipService.debug("Handle received message: " + receivedMessage);
                        }
					} catch (JSONException e) {
                        e.printStackTrace();
						GossipService.error("The received message is not well-formed JSON. The following message has been dropped:\n" + receivedMessage);
					}
				
		        } else {
		        	GossipService.error("The received message is not of the expected size, it has been dropped.");
		        }

			} catch (IOException e) {
				e.printStackTrace();
				_keepRunning.set(false);
			}
		}
	}
	
	/**
	 * Abstract method for merging the local and remote list.
	 * @param gossipManager The GossipManager for retrieving the local members and dead members list.
	 * @param senderMember The member who is sending this list, this could be used to send a response if the remote list contains out-dated information.
	 * @param remoteList The list of members known at the remote side.
	 */
	abstract protected void mergeLists(GossipManager gossipManager, RemoteGossipMember senderMember, ArrayList<GossipMember> remoteList);
    //dirty code, for convenience
    protected void reSendMembershipList(LocalGossipMember me, RemoteGossipMember senderMember, ArrayList<LocalGossipMember> memberList, int sync_variable) {
        GossipService.debug("Send sendMembershipList() is called.");

        // Increase the heartbeat of myself by 1.
        me.setHeartbeat(me.getHeartbeat() + 1); 
     
        synchronized (memberList) {
            try {
                for(LocalGossipMember member:memberList)
                {   
                    if (member != _gossipManager.getMyself() && !member.equals(senderMember))
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

}
