package com.google.code.gossip.manager;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.Console;
import java.lang.String;
import java.util.*;
import java.io.*;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;

/**
 * [The active thread: periodically send gossip request.]
 * The class handles gossiping the membership list.
 * This information is important to maintaining a common
 * state among all the nodes, and is important for detecting
 * failures.
 */
abstract public class ActiveGossipThread implements Runnable {
	
	protected GossipManager _gossipManager;
    protected int _message_id;
	
	private AtomicBoolean _keepRunning;

	public ActiveGossipThread(GossipManager gossipManager) {
		_gossipManager = gossipManager;
		_keepRunning = new AtomicBoolean(true);
	}

	@Override
	public void run() {
		while(_keepRunning.get()) {
			try {
                Console console = System.console();
				//TimeUnit.MILLISECONDS.sleep(_gossipManager.getSettings().getGossipInterval());
                String cmd = console.readLine("Input your command (exit/status/send [message]/sendme [message]/):");
                if(cmd.equals("exit"))
                {
                    System.exit(0);
                }
                else if(cmd.equals("status"))
                {
                    _gossipManager.getMyself()._sync_variable += 1;
				    sendMembershipList(_gossipManager.getMyself(), _gossipManager.getMemberList());
				    //sendMembershipList(_gossipManager.getMyself(), _gossipManager.getDeadList());
                }
                else if(cmd.startsWith("sendme"))
                {
                    String text = cmd.split(" ",2)[1];
                    sendmeMessage(_gossipManager.getMyself(),text);
                }
                else if(cmd.startsWith("send"))
                {
                    String text = cmd.split(" ",2)[1];
                    //sendmeMessage(_gossipManager.getMyself(),text);
                    sendMessage(_gossipManager.getMyself(), text);
                }
                else if(cmd.startsWith("readfile"))
                {
                    Vector<String> cmds = new Vector<String>();
                    String filename = cmd.split(" ")[1];
                    File file = new File(filename);
                    BufferedReader reader = null;
                    try 
                    {
                        reader = new BufferedReader(new FileReader(file));
                        String tempString;
                        while ((tempString = reader.readLine()) != null)
                        {
                            cmds.addElement(tempString);
                        }
                    }
                    catch(IOException e)
                    {}
                    for(String tmp : cmds)
                    {   
                        System.out.println(tmp);
                        //sendmeMessage(_gossipManager.getMyself(),tmp);
                        sendMessage(_gossipManager.getMyself(),tmp);
                        Thread.sleep(1000);
                    }   
                }
                else
                {
                    continue;
                }
			} catch (Exception e) {
				// This membership thread was interrupted externally, shutdown
				GossipService.debug("The ActiveGossipThread was interrupted externally, shutdown.");
				e.printStackTrace();
				_keepRunning.set(false);
			}
		}

		_keepRunning = null;
	}
	
	/**
	 * Performs the sending of the membership list, after we have
	 * incremented our own heartbeat.
	 */
	abstract protected void sendMembershipList(LocalGossipMember me, ArrayList<LocalGossipMember> memberList);
	abstract protected void sendMessage(LocalGossipMember me, String text);
	abstract protected void sendmeMessage(LocalGossipMember me, String text);

	/**
	 * Abstract method which should be implemented by a subclass.
	 * This method should return a member of the list to gossip with.
	 * @param memberList The list of members which are stored in the local list of members.
	 * @return The chosen LocalGossipMember to gossip with.
	 */
	abstract protected LocalGossipMember selectPartner(ArrayList<LocalGossipMember> memberList);
}
