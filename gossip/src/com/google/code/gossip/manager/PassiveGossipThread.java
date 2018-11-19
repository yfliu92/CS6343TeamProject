package com.google.code.gossip.manager;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.String;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.manager.Ring.*;

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

    private ServerSocket _ss;

	public PassiveGossipThread(GossipManager gossipManager) {
		_gossipManager = gossipManager;
		
		// Start the service on the given port number.
		try {
			//_server = new DatagramSocket(_gossipManager.getMyself().getPort());
			
			// The server successfully started on the current port.
			GossipService.info("Gossip service successfully initialized on port " + _gossipManager.getMyself().getPort());
			GossipService.debug("I am " + _gossipManager.getMyself());
            _ss = new ServerSocket(_gossipManager.getMyself().getPort());
        } catch (SocketException ex) {
			// The port is probably already in use.
			_server = null;
			// Let's communicate this to the user.
			GossipService.error("Error while starting the gossip service on port " + _gossipManager.getMyself().getPort() + ": " + ex.getMessage());
			System.exit(-1);
		}
        catch (Exception e)
        {
            GossipService.error("IOException");
        }
		
		_keepRunning = new AtomicBoolean(true);
	}

	@Override
	public void run() {
        GossipService.debug("I am in passive Thread of GossipThread" + _gossipManager.getMyself().getHost() + " "+ _gossipManager.getMyself().getPort());
        ProxyServer proxy = new ProxyServer(_gossipManager.getMyself().getHost(), _gossipManager.getMyself().getPort());
        //Initialize the ring cluster
        proxy.initializeRing_gossip();
        DataStore dataStore = new DataStore();
		while(_keepRunning.get()) {
		    try {
                Socket s = _ss.accept();
                GossipService.debug("A new client is connected : " + s);
                BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter output = new PrintWriter(s.getOutputStream(), true);
                String msg = null;
                while(true)
                {
                    msg = input.readLine();
                    if(msg == null || msg.equals(""))
                    {
                        break;
                    }
					// Extract the members out of the packet
					String receivedMessage = msg;
					//GossipService.debug("Received message (" + packet_length + " bytes): " + receivedMessage);
                    GossipService.debug("Handle received message: " + receivedMessage);
					try {
						
						ArrayList<GossipMember> remoteGossipMembers = new ArrayList<GossipMember>();
						
						RemoteGossipMember senderMember = null;
                        String[] tmp_split = receivedMessage.split(";");
						if(tmp_split[0].startsWith("Sync:"))
                        {
						GossipService.debug("Received member list:");
						// Convert the received JSON message to a JSON array.
                        int sync_variable = Integer.parseInt(tmp_split[0].split(":")[1]);
						JSONArray jsonArray = new JSONArray(tmp_split[1]);
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
                                    senderMember._sync_variable = sync_variable;
								}
								remoteGossipMembers.add(member);
							} else {
								GossipService.error("The received member object does not contain 3 objects:\n" + memberJSONObject.toString());
							}
						}
						// Merge our list with the one we just received
						mergeLists(_gossipManager, senderMember, remoteGossipMembers);
                        // Gossip spread among all servers
						GossipService.debug("sync_variable:" + sync_variable + " _gossipManager.myself.sync_variable:" + _gossipManager.getMyself()._sync_variable);
                        if(sync_variable > _gossipManager.getMyself()._sync_variable)
                        {
                            _gossipManager.getMyself()._sync_variable = sync_variable;
                            reSendMembershipList(_gossipManager.getMyself(), senderMember, _gossipManager.getMemberList());
                        }
                        }
                        else if(tmp_split[0].startsWith("Resend:"))
                        {
                            if(tmp_split.length > 1)
                            {
                                Thread t = proxy.new ClientHandler(s, tmp_split[1], output, proxy, dataStore);
                                t.start();
                            }
                        }
                        else
                        {
                            Thread t = proxy.new ClientHandler(s, tmp_split[0], output, proxy, dataStore);
                            t.start();
                            reSendMessage(_gossipManager.getMyself(), _gossipManager.getMemberList(), tmp_split[0]);
                        }
					} catch (JSONException e) {
                        e.printStackTrace();
						GossipService.error("The received message is not well-formed JSON. The following message has been dropped:\n" + receivedMessage);
					}
                    catch (Exception e){
                        s.close(); 
                        e.printStackTrace();
                    }
                }
                s.close();
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
    protected void reSendMessage(LocalGossipMember me, ArrayList<LocalGossipMember> memberList,String text)
    {
        GossipService.debug("reSendMessage() is called.");
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

    protected void reSendMembershipList(LocalGossipMember me, RemoteGossipMember senderMember, ArrayList<LocalGossipMember> memberList) {
        GossipService.debug("reSendMembershipList() is called.");

        // Increase the heartbeat of myself by 1.
        me.setHeartbeat(me.getHeartbeat() + 1); 
     
        synchronized (memberList) {
            //try {
                for(LocalGossipMember member:memberList)
                {   
                    try{
                    if (member != _gossipManager.getMyself() && !member.equals(senderMember) && me._sync_variable > member._sync_variable)
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
					String sending_message = "Sync:" + me._sync_variable + ";" + jsonArray.toString();
					GossipService.debug("Sending message: " + sending_message);
                    SocketAddress socketAddress = new InetSocketAddress(dest, member.getPort());
                    Socket socket = new Socket();
                    socket.connect(socketAddress, 1000);
                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter output = new PrintWriter(outputStream, true);
                    output.println(sending_message);
                    output.flush();
					socket.close();
			        } catch (IOException e1) {
                        GossipService.debug("Connection Failed.");
				        //e1.printStackTrace();
			        }
                }
            /*} catch (IOException e1) {
                GossipService.debug("Connection Failed");
                //e1.printStackTrace();
            }*/
        }
    }

}
