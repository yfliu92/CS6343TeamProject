package com.google.code.gossip.manager;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.Console;
import java.lang.String;

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
                String cmd = console.readLine("Input your command (exit/status/send [message]/):");
                if(cmd.equals("exit"))
                {
                    System.exit(0);
                }
                else if(cmd.equals("status"))
                {
                    _gossipManager.sync_variable += 1;
				    sendMembershipList(_gossipManager.getMyself(), _gossipManager.getMemberList(), _gossipManager.sync_variable);
				    //sendMembershipList(_gossipManager.getMyself(), _gossipManager.getDeadList());
                }
                else if(cmd.startsWith("send"))
                {
                    String text = cmd.split(" ",2)[1];
                    sendMessage(_gossipManager.getMyself(), _gossipManager.getMemberList(),text);
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
	abstract protected void sendMembershipList(LocalGossipMember me, ArrayList<LocalGossipMember> memberList, int sync_variable);
	abstract protected void sendMessage(LocalGossipMember me, ArrayList<LocalGossipMember> memberList,String text);

	/**
	 * Abstract method which should be implemented by a subclass.
	 * This method should return a member of the list to gossip with.
	 * @param memberList The list of members which are stored in the local list of members.
	 * @return The chosen LocalGossipMember to gossip with.
	 */
	abstract protected LocalGossipMember selectPartner(ArrayList<LocalGossipMember> memberList);
}
