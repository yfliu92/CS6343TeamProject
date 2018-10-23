/**
 * control_client.java
 *
 * DHT1: Cassandra and Swift, DHT ring
 * DHT2: Ceph,rush and crush
 * DHT3: Elastic DHT: Similar to Redis 
 *
 *  (c) 2018 Li Jincheng
 */
package control_client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Date;
import java.util.HashMap;
import java.io.Console;
import java.util.Vector;

import java.lang.String;
import java.io.File;
import java.io.FileReader;

import dht.server.Command;

interface dht_table
{
    /*
    * Physical node insertion and removal
    * Virtual node insertion and removal
    * Load balance request (see load balance section)
    * Report: Access the data structures and print them to show the correctness of the implementation
     */
	public void initialize();
    public void insert_physical_node();
    public void remove_physical_node(int node);
    public Vector query_physical_node();
    public void insert_virtual_node();
    public void remove_virtual_node(int node);
    public Vector query_virtual_node();
    public Vector query_loadbalance();
    public void do_loadbalance();
    public String report_datastructure();
}

class DHT_example implements dht_table
{
    /*
    * Physical node insertion and removal
    * Virtual node insertion and removal
    * Load balance request (see load balance section)
    * Report: Access the data structures and print them to show the correctness of the implementation
     */
	public void initialize() {
		System.out.println("Starting send request dht initialized");
	}
    public void insert_physical_node()
    {
        System.out.println("Starting send request insert_physical_node");
    }
    public void remove_physical_node(int node)
    {
        System.out.println("Starting send request remove_physical_node");
    }
    public Vector query_physical_node()
    {
        System.out.println("Starting send request query_physical_node");
        return new Vector();
    }
    public void insert_virtual_node()
    {
        System.out.println("Starting send request insert_virtual_node");
    }
    public void remove_virtual_node(int node)
    {
        System.out.println("Starting send request remove_virtual_node");
    }
    public Vector query_virtual_node()
    {
        System.out.println("Starting send request query_virtual_node");
        return new Vector();
    }
    public Vector query_loadbalance()
    {
        System.out.println("Starting send request query_loadbalance");
        return new Vector();
    }
    public void do_loadbalance()
    {
        System.out.println("Starting send request do_loadbalance");
    }
    public String report_datastructure()
    {
        System.out.println("Starting send request report_datastructure");
        return "report haha";
    }
}

class DHT_Ring implements dht_table
{
    /*
    * Physical node insertion and removal
    * Virtual node insertion and removal
    * Load balance request (see load balance section)
    * Report: Access the data structures and print them to show the correctness of the implementation
     */
//	PhysicalNode ring;
	
	public void initialize() {
//		ring = new PhysicalNode();
		System.out.println("dht ring initialized");
	}
    public void insert_physical_node()
    {
    	
        System.out.println("Starting send request insert_physical_node");
    }
    public void remove_physical_node(int node)
    {
        System.out.println("Starting send request remove_physical_node");
    }
    public Vector query_physical_node()
    {
        System.out.println("Starting send request query_physical_node");
        return new Vector();
    }
    public void insert_virtual_node()
    {
        System.out.println("Starting send request insert_virtual_node");
    }
    public void remove_virtual_node(int node)
    {
        System.out.println("Starting send request remove_virtual_node");
    }
    public Vector query_virtual_node()
    {
        System.out.println("Starting send request query_virtual_node");
        return new Vector();
    }
    public Vector query_loadbalance()
    {
        System.out.println("Starting send request query_loadbalance");
        return new Vector();
    }
    public void do_loadbalance()
    {
        System.out.println("Starting send request do_loadbalance");
    }
    public String report_datastructure()
    {
        System.out.println("Starting send request report_datastructure");
        return "report haha";
    }
}


public class control_client {
    //Table initialization
	
    PrintWriter out;
    BufferedReader input;
	SocketAddress socketAddress;
	Socket socket;
	
    public static dht_table initialize_DHT1()
    {
        //initialize_JSONRouter();
    	
    	
        return new DHT_Ring();
    }
    public static dht_table initialize_DHT2()
    {
        //initialize_JSONRouter();
        return new DHT_example();
    }
    public static dht_table initialize_DHT3()
    {
        //initialize_JSONRouter();
        return new DHT_example();
    }
    
    
    public boolean connectServer(String serverAddress, int port) {
//    	control_client client = new control_client();
    	int timeout = 2000;
		try {
			socketAddress = new InetSocketAddress(serverAddress, port);
			socket = new Socket();
			socket.connect(socketAddress, timeout);
	        out = new PrintWriter(socket.getOutputStream(), true);
	        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        
	        System.out.println("connected to : " + serverAddress + ":" + port);
//			socket.close();
			return true;
 
		} catch (SocketTimeoutException exception) {
//			socket.close();
			System.out.println("SocketTimeoutException " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		} catch (IOException exception) {
//			socket.close();
			System.out.println(
					"IOException - Unable to connect to " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		}
    }
    
    public String sendCommand(String command) throws IOException {
    	String timeStamp = new Date().toString();
    	System.out.println("sending command" + " ---- " + timeStamp);
            out.println(command);
        String response = input.readLine();
//        JOptionPane.showMessageDialog(null, answer);
        timeStamp = new Date().toString();
        System.out.println("response Received: " + response + " ---- " + timeStamp);
        
        return response;
    }
    
    public static void main (String args[]) throws IOException{
    	control_client client = new control_client();

    	String serverAddress = "localhost";
    	int port = 9090; 
    	
        System.out.println("==== Welcome to Control Client !!! =====");
        System.out.println("==== There are three types of DHT solutions, please select one(1,2,3) =====\n");
        System.out.println("DHT1: Cassandra and Swift, DHT ring");
        System.out.println("DHT2: Ceph,rush and crush ");
        System.out.println("DHT3: Elastic DHT: Similar to Redis\n");
        Console console = System.console();
        String dht = console.readLine("Please select from: 1 , 2 or 3:");
        dht_table DHT_server = new DHT_example();
        if(dht.equals("1"))
        {
            DHT_server = initialize_DHT1();
            port = 9091;
        }
        if(dht.equals("2"))
        {
            DHT_server = initialize_DHT2();
            port = 9092;
        }
        if(dht.equals("3"))
        {
            DHT_server = initialize_DHT3();
            port = 9093;
        }
        
        System.out.println("dht type" + dht);
        
		boolean connected = client.connectServer(serverAddress, port);
		
		if (connected) {
			System.out.println("Connected to Server " + serverAddress + ":" + port);
		}
		else {
			System.out.println("Unable to connect to server!");
			return;
		}
        
        Vector<String> cmds = new Vector<String>();
        while(true)
        {
            if(cmds.isEmpty() == true)
            {
                String cmd = console.readLine("Input your command (help/exit/read file/add **/remove **/loadbalance **/info):");
                cmds.addElement(cmd);
            }
            String cmd = cmds.remove(0);
            Command command = new Command(cmd);
            System.out.println(cmd);
            if(command.getAction().equals("help"))
            {
                System.out.println("--> insert/remove/query physical/virtual [node]");
                System.out.println("--> query/do loadbalance");
                System.out.println("--> report datastructure");
            }
            else if(command.getAction().equals("exit"))
            {
                System.exit(0);
            }
            else if(command.getAction().startsWith("read"))
            {
                String filename = cmd.split(" ")[1];
                System.out.println(filename);
                File file = new File(filename);
                BufferedReader reader = null;
                try 
                {
                    reader = new BufferedReader(new FileReader(file));
                    String tempString;
                    while ((tempString = reader.readLine()) != null)
                    {
                        System.out.println(tempString);
                        cmds.addElement(tempString);
                    }
                }
                catch(IOException e)
                {
                }
                finally
                {
                    if(reader != null)
                    {
                        try 
                        {  
                            reader.close();  
                        } 
                        catch (IOException e1) 
                        {  
                        } 
                    }
                }
            }
            else
            {
//                switch(command.getAction())
//                {
//                    case "add":
//                        DHT_server.insert_physical_node();
//                        break;
//                    case "remove":
//                        DHT_server.remove_physical_node(0);
//                        break;
//                    case "find":
//                        DHT_server.query_physical_node();
//                        break;
//                    case "addvirt":
//                        DHT_server.insert_virtual_node();
//                        break;
//                    case "removevirt":
//                        DHT_server.remove_virtual_node(0);
//                        break;
//                    case "findvirt":
//                        DHT_server.query_virtual_node();
//                        break;
//                    case "info":
//                        DHT_server.query_loadbalance();
//                        break;
//                    case "loadbalance":
//                        DHT_server.do_loadbalance();
//                        break;
//                    case "report datastructure":
//                        DHT_server.report_datastructure();
//                        break;
//                }
            	
            	String response = client.sendCommand(cmd);
            }
        }
    }
}
