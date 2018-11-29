package com.google.code.gossip.manager.Ring;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;
import javax.json.*;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import com.google.code.gossip.manager.Ring.*;

public class ProxyServer extends PhysicalNode {
    public static final int WRITE_BATCH_SIZE = 1000;
	public static int numOfReplicas;
	public static int phy_nodes_num;
	public static int vir_nodes_num;
    public static int balance_level;

	public ProxyServer(String ip, int port){
		super(ip,port);
	}

	public void initializeRing_gossip(){
        try {
            // Read from the configuration file "config_ring.xml"
			String xmlPath = System.getProperty("user.dir") + "/com/google/code/gossip/manager/Ring/" + "config_ring.xml";
            System.out.println(xmlPath);
            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            // Read the elements in the configuration file
			numOfReplicas = Integer.parseInt(document.getRootElement().element("replicationLevel").getStringValue());
			phy_nodes_num = Integer.parseInt(document.getRootElement().element("physical").getStringValue());
			vir_nodes_num = Integer.parseInt(document.getRootElement().element("virtual").getStringValue());
            balance_level = (int)(vir_nodes_num / phy_nodes_num / 10);
            Hashing.MAX_HASH = vir_nodes_num;
            Element nodes = document.getRootElement().element("nodes");
            List<Element> listOfNodes = nodes.elements();

            int physical_machine = listOfNodes.size();
            int each_machine_physical = phy_nodes_num / physical_machine;
            int each_physical_virtual = vir_nodes_num / phy_nodes_num;
            int count_Replicas = each_physical_virtual * (numOfReplicas - 1);
            BinarySearchList table = new BinarySearchList(this);

            for (int i = 0; i < physical_machine; i++){
                String ip = listOfNodes.get(i).element("ip").getStringValue();
                int port = Integer.parseInt(listOfNodes.get(i).element("port").getStringValue());
                //System.out.println(ip + " " +port);
                for (int j = 0; j < each_machine_physical; j++)
                {
                    int start_vir = each_physical_virtual * (i * each_machine_physical + j);
                    int end_vir = each_physical_virtual * (i * each_machine_physical + j + 1) - 1;
                    int back_end_vir = table.pre_vir(start_vir, 1);
                    int back_start_vir = table.pre_vir(start_vir, count_Replicas);
                    //System.out.println("super.address:" + super.address + " port:" + super.port + " ip:" + ip + " port:" + port);
                    if(!ip.equals(super.address) || port + j != super.port)
                    {
                        PhysicalNode node = new PhysicalNode(ip, port + j, start_vir, end_vir, back_start_vir, back_end_vir);
                        table.add(node);
                    }
                    else
                    {
                        super.start_vir = start_vir;
                        super.end_vir = end_vir;
                        table.add(this);
                    }
                }
            }

            table.updateIndex();
            
            // Create a lookupTable and set it to every physical node
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            super.epoch = timestamp.getTime();
            
            super.physicalNodes = table;

			System.out.println("Initialized successfully: " + physicalNodes.size() + " physical nodes, " + vir_nodes_num + " virtual nodes");
        }catch(DocumentException e) {
        	System.out.println("Failed to initialize");
            e.printStackTrace();
        }
    }

	public void CCcommands(int total_CCcommands){
		Writer writer = null;

		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("ring_CCcommands.txt"), "utf-8"));
			String[] availableCommands = {"add1", "add2", "remove1", "remove2", "loadbalance"};
			String[] availableIPs = {"192.168.0.211","192.168.0.212","192.168.0.213","192.168.0.214",
					"192.168.0.215","192.168.0.216","192.168.0.217","192.168.0.218","192.168.0.219","192.168.0.220",
					"192.168.0.221", "192.168.0.222","192.168.0.223","192.168.0.224","192.168.0.225",
					"192.168.0.226", "192.168.0.227","192.168.0.228","192.168.0.229","192.168.0.230"};
			String[] availablePorts = {"8001", "8002", "8003", "8004", "8005"};
			ArrayList<String> availablePNodes = new ArrayList<>();
			for (String ip : availableIPs){
				for (String port : availablePorts){
					availablePNodes.add(ip + " " + port);
				}
			}
			// Get the current physicalIDs after initialization
			Collection<PhysicalNode> pNodes = super.physicalNodes;
			List<PhysicalNode> currentPNodes = new ArrayList<>();
			for (PhysicalNode node : pNodes){
				currentPNodes.add(node);
			}
			List<Integer> currentVNodes = new ArrayList<>();
            //TODO

			// Write control client commands into the "ring_CCcommands.txt" file (in the "/src" folder by default)
			for (int i = 0; i < total_CCcommands; i++){
				// Randomly pick a command from available commands
				Random ran = new Random();
				String command = availableCommands[ran.nextInt(availableCommands.length)];

				// add1 means to add a virtual node for an existing physical node
				// Use addNode(String ip, int port, int hash) for this command
				if (command.equals("add1")){
					if (currentPNodes.size() == 0){
						continue;
					}
					String ip_port = currentPNodes.get(ran.nextInt(currentPNodes.size())).getId();
					String[] lst = ip_port.split("-");
					int ran_hash;
					do {
						ran_hash = ran.nextInt(vir_nodes_num);
					} while (currentVNodes.contains(ran_hash));
					currentVNodes.add(ran_hash);
					writer.write("add " + lst[0] + " " + lst[1] + " " + ran_hash + "\n");
				}

				// add2 means to add a new physical node and map it to more than 1 virtual node
				// Use addNode(String ip, int port, int[] hashes) for this command
				else if (command.equals("add2")){
					if (availablePNodes.size() == 0){
						continue;
					}
					String ip_port = availablePNodes.get(ran.nextInt(availablePNodes.size()));
					String[] lst = ip_port.split(" ");
					String ip = lst[0];
					int port = Integer.parseInt(lst[1]);
					availablePNodes.remove(ip_port);
					Set<Integer> hashes = new HashSet<>();
                    //TODO
					while (hashes.size() < vir_nodes_num) {
						int ran_hash;
						do {
							ran_hash = ran.nextInt(vir_nodes_num);
						} while (currentVNodes.contains(ran_hash));
						hashes.add(ran_hash);
						currentVNodes.add(ran_hash);
					}
					PhysicalNode newNode = new PhysicalNode(ip, port);
					currentPNodes.add(newNode);
					writer.write("add " + ip_port + "\n");
				}

				// remove1 means to remove a virtual node
				// use deleteNode(int hash) for this command
				else if (command.equals("remove1")){
					if (currentVNodes.size() == 0){
						continue;
					}
					int ran_hash = currentVNodes.get(ran.nextInt(currentVNodes.size())) ;
					currentVNodes.remove(Integer.valueOf(ran_hash));
					writer.write("remove " + ran_hash + "\n");
				}

				// remove2 means to remove a physical node and all its corresponding virutal nodes
				// use failNode(String ip, int port) for this command
				else if (command.equals("remove2")){
					if (currentPNodes.size() == 0){
						continue;
					}
					PhysicalNode node_to_delete = currentPNodes.get(ran.nextInt(currentPNodes.size()));
					String[] lst = node_to_delete.getId().split("-");
					String id = lst[0] + " " + lst[1];
					currentPNodes.remove(node_to_delete);
					availablePNodes.add(id);
					writer.write("remove " + id + "\n");
				}
				// use loadBalance(int delta, int hash) for this command
				else if (command.equals("loadbalance")) {
					if (currentVNodes.size() == 0){
						continue;
					}
					int ran_hash = currentVNodes.get(ran.nextInt(currentVNodes.size())) ;
					int ran_delta;
					do {
						ran_delta = ran.nextInt(200) - 100;
					} while (ran_delta == 0);

					writer.write("loadbalance " + ran_delta + " " + ran_hash + "\n");
				}
			}
		} catch (IOException ex) {
			// Report
		} finally {
			try {writer.close();} catch (Exception ex) {/*ignore*/}
		}
	}

	public String getResponse(String commandStr, DataStore dataStore) {
		Command command = new Command(commandStr);
		try {
			if (command.getAction().equals("read")) 
            {
				String dataStr = command.getCommandSeries().get(0);
    			int rawhash = Hashing.getHashValFromKeyword(dataStr);
    			String rlt = super.read(rawhash);
    			return new Response(true, rlt, "read file " + dataStr + " from server").serialize();
			}
			else if (command.getAction().equals("write")) {
				String dataStr = command.getCommandSeries().get(0);
    			int rawhash = Hashing.getHashValFromKeyword(dataStr);
    			String rlt = super.write(rawhash);
    			return new Response(true, rlt, "write file " + dataStr + " from server").serialize();
			}
			else if (command.getAction().equals("data")) {
				return dataStore.listDataRes();
			}
			else if (command.getAction().equals("loadbalance")) {
				int index = Integer.valueOf(command.getCommandSeries().get(0));
				return super.loadBalance(index);
			}
			else if (command.getAction().equals("add")) {
				String ip = command.getCommandSeries().get(0);
				int port = Integer.valueOf(command.getCommandSeries().get(1));
				int hash = -1;
                String result;
                //System.out.println(ip + " " + port + " " + command.getCommandSeries().size());
                if(command.getCommandSeries().size() == 3)
                {
                    hash = Integer.valueOf(command.getCommandSeries().get(2));
				    result = super.addNode(ip, port, hash);
                }
                else
                {
                    result = super.addNode(ip, port);
                }
				return result;
			}
			else if (command.getAction().equals("remove")) {
				int index = Integer.valueOf(command.getCommandSeries().get(0));
				String result = super.deleteNode(index);
				return result;
			}
			else if (command.getAction().equals("find")) {
				int hash = Hashing.getHashValFromKeyword(command.getCommandSeries().get(0));
                System.out.println(command.getCommandSeries().get(0) + "  hash:" + hash);
				return new Response(true, physicalNodes.get(physicalNodes.find(hash)).toJSON(), "Physical Node Info at Server").serialize();
			}
			else if (command.getAction().equals("info")) {
//				return new Response(true, super.listNodes()).serialize();
				return new Response(true, super.physicalNodes.toJSON(), "DHT Table from Server", physicalNodes.epoch).serialize();
			}
			else if (command.getAction().equals("update")) {
                String result = super.updateNode(commandStr.split(" ",2)[1]);
                return result;
            }
			else if (command.getAction().equals("dht")) {
				String operation = command.getCommandSeries().size() > 0 ? command.getCommandSeries().get(0) : "head";
				if (operation.equals("head")) {
					return new Response(true, String.valueOf(super.getEpoch()), "Current epoch number:").serialize();
				}
				else if (operation.equals("pull")) {
					return new Response(true, super.physicalNodes.toJSON(), "Ring DHT table").serialize();
				}
				else if (operation.equals("print")) {
					super.print();
					return new Response(true, "DHT printed on server").serialize();
				}
				else {
					return new Response(false, "Command not supported").serialize();
				}
			}
			else {
				return "Command not supported";
			}
		}
		catch (Exception ee) {
			return "Illegal command " + ee.toString();
		}
	}
	
	public class ClientHandler extends Thread  
	{
	    final String msg;
	    final PrintWriter output;
	    final Socket s; 
	    final ProxyServer proxy;
	    final DataStore dataStore;
	  
	    public ClientHandler(Socket s, String msg, PrintWriter output, ProxyServer proxy, DataStore dataStore) {
	    	this.s = s; 
	    	this.msg = msg;
	        this.output = output;
	        this.proxy = proxy;
	        this.dataStore = dataStore;
	    }
	  
	    @Override
	    public void run()  
	    { 
	            try {
	                if (msg == null) {
                		System.out.println("Connection end " + " ---- " + new Date().toString());
	                }
	                  
	                if (msg.equals("Exit")) 
	                {  
	                    System.out.println("Client " + this.s + " sends exit..."); 
	                    System.out.println("Closing this connection."); 
	                    this.s.close(); 
	                    System.out.println("Connection closed by " + s.getPort()); 
	                }
	                else { // msg != null
                    	System.out.println("Request received from " + s.getPort() + ": " + msg + " ---- " + new Date().toString());
                    	System.out.println();
                    	
                    	String response = proxy.getResponse(msg, dataStore);

                    	output.println(response);
                    	output.flush();
                    	
                        System.out.println("Response sent to " + s.getPort() + ": " + response + " ---- " + new Date().toString());
                        System.out.println();
                	}
              
	            } catch (IOException e) { 
	            	// System.out.println("Connection reset at " + s.getPort() + " ---- " + new Date().toString());
	                // e.printStackTrace(); 
	            } 
	    } 
	}
	
    public static void main(String[] args) throws IOException { 
		ProxyServer proxy = new ProxyServer("",0);
		//Initialize the ring cluster
		proxy.initializeRing_gossip();
		//proxy.CCcommands();
		DataStore dataStore = new DataStore();
		int port = 9091;
        ServerSocket ss = new ServerSocket(port); 
        System.out.println("Ring server running at " + String.valueOf(port));
          
        while (true)  
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected : " + s); 
                  
            	BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
		        PrintWriter output = new PrintWriter(s.getOutputStream(), true);
//		        
//            	output.println(s.getPort());
//            	output.flush();
  
                Thread t = proxy.new ClientHandler(s, input.readLine(), output, proxy, dataStore); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
    } 
}
