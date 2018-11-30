package dht.rush;

import dht.common.response.Response;
import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;
import dht.rush.commands.*;
import dht.rush.utils.ConfigurationUtil;
import dht.rush.utils.GenerateControlClientCommandUtil;
import dht.rush.utils.StreamUtil;
import dht.server.Command;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class CentralServer {
    private Cluster root;
    private ClusterStructureMap clusterStructureMap;
    Document config;
    int port;
    String IP;
    public static int hashRange;
    
    public ClusterStructureMap getClusterMap() {
    	return this.clusterStructureMap;
    }
    
    public Cluster getRoot() {
    	return this.root;
    }
    
	public static void runDataNodeBatch(String thisIP) {
		String rootPath = System.getProperty("user.dir");
        String xmlPath = rootPath + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
//      String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
        File inputFile = new File(xmlPath);
        SAXReader reader = new SAXReader();
        
        Document config = null;
        try {
        	config = reader.read(inputFile);
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        Element rootElement = config.getRootElement();
        int startPort = Integer.parseInt(rootElement.element("subClusters").element("subCluster").element("port").getStringValue());
        int portRange = Integer.parseInt(rootElement.element("offset").getStringValue());
        
		for (int j = 0; j < portRange; j++){
			int portNum = startPort + j;
			System.out.println(portNum);
	    	Thread t = new RunDataNode_Rush(thisIP, portNum, hashRange);
	    	t.start();
		}
	}

    public static void main(String[] args) {
    	
    	if (args.length > 0) {
    		if (args.length == 3 || args.length == 2) {
    			String ip = args.length == 3 ? args[2] : "localhost";
    			if (args[0].equals("run") && args[1].equals("datanode")) {
    	    		runDataNodeBatch(ip);
    	    		System.out.println("All data nodes on the machine are running...");
    			}
    			else {
        			System.out.println("Input commands:");
        			System.out.println("run datanode <IP>");
        			System.out.println("run datanode");
    			}
    		}
    		else {
    			System.out.println("Input commands:");
    			System.out.println("run datanode <IP>");
    			System.out.println("run datanode");
    		}

    		return;
    	}
    	
        CentralServer cs = new CentralServer();
        String rootPath = System.getProperty("user.dir");
//        String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";

        String xmlPath = rootPath + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";

        cs.initializeRush(xmlPath);
        cs.clusterStructureMap = ConfigurationUtil.parseConfig(cs.config);

        if (cs.clusterStructureMap == null) {
            System.out.println("Central Server initialization failed");
            System.exit(-1);
        }

        /**
         * Will generate the control client commands, will be executed only one time, after generating the commands, comment the following code
         */
//        GenerateControlClientCommandUtil.setMap(cs.clusterStructureMap);
//        GenerateControlClientCommandUtil.run();

        cs.root = cs.clusterStructureMap.getChildrenList().get("R");
//        System.out.println(cs.clusterStructureMap.toJSON().toString());
        
        cs.initializeDataNode(cs.root);
        
//        int port = cs.port;
//        String serverAddress = cs.IP;
        try {
            cs.startup(cs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void initializeRush(String xmlPath){
    	try {
            System.out.println(xmlPath);
            File inputFile = new File(xmlPath);
            SAXReader reader = new SAXReader();
            config = reader.read(inputFile);
            port = Integer.parseInt(config.getRootElement().element("proxy").element("port").getStringValue());
            IP = config.getRootElement().element("proxy").element("ip").getStringValue();
            hashRange = Integer.valueOf(config.getRootElement().element("placementGroupNumber").getStringValue());
        }catch(Exception e) {
        	System.out.println("Failed to initialize");
            e.printStackTrace();
        }
    }
    
	public void initializeDataNode(Cluster root) {
        if (root == null) {
        	return;
        }
    
        try {
        	if (!root.getId().equals("R") && !root.getPort().equals("")) {
        		pushDHT(root.getIp(), Integer.valueOf(root.getPort()));
        		System.out.println("DHT successfully pushed to Data Node " + root.getIp() + ":" + root.getPort());
        	}
        }
        catch (Exception e){
        	System.out.println("DHT failed to push to Data Node " + root.getIp() + ":" + root.getPort());
//        	System.out.println(root.toJSON());
        }
        
        if (root.getSubClusters() != null && root.getSubClusters().size() > 0) {
        	for(Cluster cluster: root.getSubClusters()) {
        		initializeDataNode(cluster);
        	}
        }
	}
	
	public boolean pushDHT(String serverAddress, int port) {
		try {
			ProxyClient_Rush client = new ProxyClient_Rush(this);
	    	boolean connected = client.connectServer(serverAddress, port);
	    	
	    	Thread t = null;
	    	if (!connected) {
	    		t = new RunDataNode_Rush(serverAddress, port, CentralServer.hashRange);
	    		t.start();
	    		
	    		Thread.sleep(1000);
	    		connected = client.connectServer(serverAddress, port);
	    	}
	    	
	    	Thread.sleep(1000);
			if (connected) {
				
				System.out.println("Connected to Data Node Server at " + serverAddress + ":" + port);
				
				JsonObject jobj = new Response(true, this.clusterStructureMap.toJSON(), "dht push").toJSON();
				
	        	client.output.println(jobj);
	        	client.output.flush();
				
		        ByteArrayOutputStream baos = new ByteArrayOutputStream();
		        JsonWriter writer = Json.createWriter(baos);
		        writer.writeObject(jobj);
		        writer.close();
		        baos.writeTo(client.outputStream);

		        client.outputStream.write("\n".getBytes());
		        client.outputStream.flush();

		        JsonObject res = parseRequest(client.input);
		        if (res != null) {
		            System.out.println();
		        	System.out.println("Response received at " + new Date() + " ---- " + res.toString());
		            if (res.containsKey("status") && res.containsKey("message")) {
		                System.out.println("REPONSE STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
		            }
		            System.out.println();
		         }
				
		        client.disconnectServer();
//		        System.out.println("Disconnected with " + serverAddress + ":" + port);
				
				return true;
			}
			else {
//				System.out.println("Unable to connect to Data Node Server at " + serverAddress + " " + port);
				return false;
			}
		}
		catch (Exception e) {
			System.out.println("Exception pushing DHT to Data Node " + serverAddress + " " + port);
			return false;
		}
		
 
	}
	
    public static JsonObject parseRequest(BufferedReader br) throws Exception {
        String str;
        JsonObject jsonObject = null;

        while ((str = br.readLine()) != null) {
            JsonReader jsonReader = Json.createReader(new StringReader(str));
            jsonObject = jsonReader.readObject();
            return jsonObject;
        }
        return jsonObject;
    }
    
	public class ClientHandler extends Thread  
	{
        final InputStream inputStream;
        final OutputStream outputStream;
        final BufferedReader input;
        final PrintWriter output;
	    final Socket s;
	    final CentralServer cs;
	  
	    public ClientHandler(CentralServer cs, Socket s, InputStream inputStream, OutputStream outputStream, BufferedReader input, PrintWriter output) {
	    	this.cs = cs;
	    	this.s = s; 
	    	this.inputStream = inputStream;
	        this.outputStream = outputStream;
	        this.input = input;
	        this.output = output;
	    }
	  
	    @Override
	    public void run()  
	    { 
	        String msg; 
	        while (true)  
	        { 
	            try {
	            	msg = input.readLine(); 
	                if (msg == null) {
                		System.out.println("Connection end " + " ---- " + new Date().toString());
                		break;
	                }
	                  
	                if (msg.equals("Exit")) 
	                {  
	                    System.out.println("Client " + this.s + " sends exit..."); 
	                    System.out.println("Closing this connection."); 
	                    this.s.close(); 
	                    System.out.println("Connection closed by " + s.getPort()); 
	                    break; 
	                }
	                else { // msg != null
	                	JsonObject requestObject = StreamUtil.parseRequest(msg);

						String requestStr = msg.length() > 200 ? msg.substring(0, 200) + "...": msg; 

                    	System.out.println("Request received from " + s.getPort() + ": " + requestStr + " ---- " + new Date().toString());
                    	System.out.println();
                    	
                      if (requestObject != null) {
	                      ServerCommand command = dispatchCommand(requestObject, this.cs);
	                      command.setInputStream(inputStream);
	                      command.setOutputStream(outputStream);
	                      command.run();
	                      
//	                      JsonObject params = requestObject.getJsonObject("parameters");
//	                      String method = requestObject.getString("method").toLowerCase();
//	                      String operation = params.containsKey("operation") ? params.getString("operation") : "";
//	                      if (method.equals("dht") && operation.equals("push") && !params.containsKey("series")) {
//	                    	  this.cs.initializeDataNode(this.cs.root);
//	                      }
//	                      else if (method.equals("addnode") || method.equals("deletenode") || method.equals("loadbalancing")) {
//	                    	  this.cs.initializeDataNode(this.cs.root);
//	                      }
	                  }
                	}
              
	            } catch (IOException e) { 
	            	// System.out.println("Connection reset at " + s.getPort() + " ---- " + new Date().toString());
	                // e.printStackTrace(); 
            		break;
	            } 
	        } 
	          
	        try
	        { 
	        	this.output.close();
	        	this.input.close();
	              
	        }catch(IOException e){ 
	            e.printStackTrace(); 
	        } 
	    } 
	}
	
	public void startup(CentralServer cs) throws IOException {
        ServerSocket ss = new ServerSocket(this.port); 
        
        System.out.println("Rush proxy server running at " + String.valueOf(port));
          
        while (true)  
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected to Proxy Server: " + s); 
                  
                InputStream inputStream = s.getInputStream();
                OutputStream outputStream = s.getOutputStream();
            	BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
		        PrintWriter output = new PrintWriter(outputStream, true);

//                Thread t = server.new ClientHandler(s, inputStream, outputStream, input, output); 
                Thread t = new ClientHandler(cs, s, inputStream, outputStream, input, output); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
	}

    private ServerCommand dispatchCommand(JsonObject requestObject, CentralServer server) throws IOException {
        String method = requestObject.getString("method").toLowerCase();
        ServerCommand serverCommand = null;
        JsonObject params = null;
        
        System.out.println("dispatch command: " + method);
        if (method.toLowerCase().equals("addnode")) {
                System.out.println("Adding node command");
                serverCommand = new AddNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((AddNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((AddNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((AddNodeCommand) serverCommand).setPort(params.getString("port"));
                ((AddNodeCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((AddNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((AddNodeCommand) serverCommand).setCentralServer(server);
        }
        else if (method.toLowerCase().equals("deletenode")) {
                System.out.println("Deleting node command");
                serverCommand = new DeleteNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((DeleteNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((DeleteNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((DeleteNodeCommand) serverCommand).setPort(params.getString("port"));
                ((DeleteNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((DeleteNodeCommand) serverCommand).setCentralServer(server);
        }
        else if (method.toLowerCase().equals("getnodes")) {
                System.out.println("Getting node command");
                serverCommand = new GetNodesCommand();
                params = requestObject.getJsonObject("parameters");
                ((GetNodesCommand) serverCommand).setPgid(params.getString("pgid"));
                ((GetNodesCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.toLowerCase().equals("loadbalancing")) {
                System.out.println("Start loading balancing in a subcluster");
                serverCommand = new LoadBalancingCommand();
                params = requestObject.getJsonObject("parameters");
                ((LoadBalancingCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((LoadBalancingCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((LoadBalancingCommand) serverCommand).setCentralServer(server);
        }
        else if (method.toLowerCase().equals("write")) {
                System.out.println("Start to write a file into the cluster");
                serverCommand = new WriteCommand();
                params = requestObject.getJsonObject("parameters");
                ((WriteCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((WriteCommand) serverCommand).setFileName(params.getString("fileName"));
                ((WriteCommand) serverCommand).setCentralServer(server);
        }
        else if (method.toLowerCase().equals("read")) {
                System.out.println("Start to return a physical node for the file");
                serverCommand = new ReadCommand();
                params = requestObject.getJsonObject("parameters");
                ((ReadCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((ReadCommand) serverCommand).setFileName(params.getString("fileName"));
        }
        else if (method.toLowerCase().equals("getmap")) {
                System.out.println("Start to get the most recent tree map");
                serverCommand = new GetMapCommand();
                ((GetMapCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.toLowerCase().equals("changeweight")) {
                System.out.println("Change node weight command");
                serverCommand = new ChangeWeightCommand();
                params = requestObject.getJsonObject("parameters");
                ((ChangeWeightCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((ChangeWeightCommand) serverCommand).setIp(params.getString("ip"));
                ((ChangeWeightCommand) serverCommand).setPort(params.getString("port"));
                ((ChangeWeightCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((ChangeWeightCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((ChangeWeightCommand) serverCommand).setCentralServer(server);
        }
        else if (method.toLowerCase().equals("dht")) {
            	System.out.println("DHT fetch command");
            	serverCommand = new GetDHTCommand();
            	params = requestObject.getJsonObject("parameters");
            	JsonArray jsonCommandSeries = params.getJsonArray("series");
            	List<String> commandSeries = new LinkedList<String>();
            	if (jsonCommandSeries != null) {
                	for(int k = 0; k < jsonCommandSeries.size(); k++) {
                		commandSeries.add(jsonCommandSeries.getString(k).toString());
                	}
            	}
            	
            	System.out.println(Arrays.toString(commandSeries.toArray()));

            	((GetDHTCommand) serverCommand).setOperation(params.getString("operation"));
            	((GetDHTCommand) serverCommand).setCommandSeries(commandSeries);
            	((GetDHTCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
            	((GetDHTCommand) serverCommand).setCentralServer(server);
        }
        else {
                System.out.println("Unknown Request " + method.toLowerCase());
        }
        return serverCommand;
    }

}

class ProxyClient_Rush{
    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
    
    Socket socket;
    CentralServer proxy;
	public ProxyClient_Rush(CentralServer proxy) { 
		this.proxy = proxy;
	}
	
    public boolean connectServer(String serverAddress, int port) {
    	int timeout = 2000;
		try {
			SocketAddress socketAddress = new InetSocketAddress(serverAddress, port);
			this.socket = new Socket();
			this.socket.connect(socketAddress, timeout);
			this.inputStream = this.socket.getInputStream();
			this.outputStream = this.socket.getOutputStream();
			this.output = new PrintWriter(this.outputStream, true);
			this.input = new BufferedReader(new InputStreamReader(this.inputStream));

	        System.out.println("Connected to server " + serverAddress + ":" + port + ", with local port " + this.socket.getLocalPort());
			return true;
 
		} catch (SocketTimeoutException exception) {
//			socket.close();
//			System.out.println("SocketTimeoutException " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		} catch (IOException exception) {
//			socket.close();
//			System.out.println(
//					"IOException - Unable to connect to " + serverAddress + ":" + port + ". " + exception.getMessage());
			return false;
		}
    }
    
    public void disconnectServer() {
    	try {
    		
    		this.input.close();
    		this.output.close();
    		this.socket.close();
    	}
    	catch (Exception e) {
//    		e.printStackTrace();
    	}
    }
    
    public void sendCommandStr_JsonRes(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
        output.println(command.getRawCommand());
        output.flush();
        
        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
            System.out.println("Response received at " + timeStamp);
            parseResponse(res, command, input, output);
        	
        	System.out.println();
         }
    }
    
    public void parseResponse(JsonObject res, Command command, BufferedReader input, PrintWriter output) throws Exception {
    	if (command.getAction().equals("read")) {
    		if (command.getCommandSeries().size() > 0) {

    			String physicalnodeid_remote = res.getString("result");
    			System.out.println("Physical Node Ids from Remote DHT: " + physicalnodeid_remote);
    			return;
    		}
    	}
    	else if (command.getAction().equals("write")) {
    		
    	}
    }
	
    public static JsonObject parseRequest(BufferedReader br) throws Exception {
        String str;
        JsonObject jsonObject = null;

        while ((str = br.readLine()) != null) {
            JsonReader jsonReader = Json.createReader(new StringReader(str));
            jsonObject = jsonReader.readObject();
            return jsonObject;
        }
        return jsonObject;
    }
    
    public void processCommandRush(String cmd) throws Exception {
    	Command command = new Command(cmd);
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	System.out.println();
        
        JsonObject params = null;
        JsonObject jobj = null;
		if(command.getAction().toLowerCase().equals("addnode")) {
			  params = Json.createObjectBuilder()
			  .add("subClusterId", command.getCommandSeries().get(0))
			  .add("ip", command.getCommandSeries().get(1))
			  .add("port", command.getCommandSeries().get(2))
			  .add("weight", command.getCommandSeries().get(3))
			  .build();
			
			  jobj = Json.createObjectBuilder()
			  .add("method", "addnode")
			  .add("parameters", params)
			  .build();
		}
		else if(command.getAction().toLowerCase().equals("deletenode")) {
	          params = Json.createObjectBuilder()
	          .add("subClusterId", command.getCommandSeries().get(0))
	          .add("ip", command.getCommandSeries().get(1))
	          .add("port", command.getCommandSeries().get(2))
	          .build();
	
	          jobj = Json.createObjectBuilder()
	          .add("method", "deletenode")
	          .add("parameters", params)
	          .build();
		}
		else if(command.getAction().toLowerCase().equals("getnodes")) {
            params = Json.createObjectBuilder()
                    .add("pgid", command.getCommandSeries().get(0))
                    .build();

            jobj = Json.createObjectBuilder()
                    .add("method", "getnodes")
                    .add("parameters", params)
                    .build();
		}
		else if (command.getAction().equals("help")) {
			System.out.println(getHelpText(2));
			return;
		}
		else {
			System.out.println("command not supported111");
			return;
		}
    	
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        writer.writeObject(jobj);
        writer.close();
        baos.writeTo(outputStream);

        outputStream.write("\n".getBytes());
        outputStream.flush();

        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
        	System.out.println("Response received at " + timeStamp + " ---- " + res.toString());
            if (res.containsKey("status") && res.containsKey("message")) {
                System.out.println("REPONSE STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
            }
            System.out.println();
         }
    }
    
    public void processCommandDHTPush() throws Exception {
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	System.out.println();
        
    	Response response = new Response(true, this.proxy.getClusterMap().toJSON(), "Rush DHT table");
        JsonObject params = null;
        JsonObject jobj = null;
		  params = Json.createObjectBuilder()
//		  .add("ip", ip)
//		  .add("port", port)
		  .add("result", response.toJSON())
		  .build();
		
		  jobj = Json.createObjectBuilder()
		  .add("method", "dhtpush")
		  .add("parameters", params)
		  .build();
    	
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonWriter writer = Json.createWriter(baos);
        writer.writeObject(jobj);
        writer.close();
        baos.writeTo(outputStream);

        outputStream.write("\n".getBytes());
        outputStream.flush();

        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
        	System.out.println("Response received at " + timeStamp + " ---- " + res.toString());
            if (res.containsKey("status") && res.containsKey("message")) {
                System.out.println("REPONSE STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
            }
            System.out.println();
         }
    }
    
    public void processCommandRing(String cmd, int dhtType) throws Exception {
        Command command = new Command(cmd);
        if(command.getAction().equals("help"))
        {
            System.out.println(getHelpText(dhtType));
        }
        else if(command.getAction().equals("exit"))
        {
            System.exit(0);
        }
        else
        {
        	sendCommandStr_JsonRes(command, input, output);
        	
        }
    }
    
    public void processCommandRing(String cmd) throws Exception {
    	processCommandRing(cmd, 1);
    }
    
    public void processCommandElastic(String cmd) throws Exception {
    	processCommandRing(cmd, 3);
    }
    
    public void processCommand(int typeDHT, String cmd) throws Exception {
    	switch(typeDHT) {
	    	case 1:
	    		processCommandRing(cmd);
	    		break;
	    	case 2:
	    		processCommandRush(cmd);
	    		break;
	    	case 3:
	    		processCommandElastic(cmd);
	    		break;
    	}
    }
    
    public static String getHelpText(int dhtType) {
    	String tip = "";
    	switch(dhtType) {
	    	case 1:
	    		tip = "\nhelp";
	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
	    		tip += "\ndht head|pull  //fetch server dht table info";
	    		tip += "\ndht info|list  //show local dht table info";
	    		tip += "\ninfo           //show server dht table info";
	    		tip += "\nexit\n";
	    		break;
	    	case 2:
	    		tip = "\nhelp";
	    		tip += "\naddnode <subClusterId> <IP> <Port> <weight>  //example: addnode S0 localhost 689 0.5";
	    		tip += "\ndeletenode <subClusterId> <IP> <Port>  //example: deletenode S0 localhost 689";
	    		tip += "\ngetnodes <pgid> | example: getnodes PG1";
	    		tip += "\ninfo";
	    		tip += "\nexit\n";
	    		break;
	    	case 3:
	    		tip = "\nhelp";
	    		tip += "\nread <randomStr>";
	    		tip += "\nfind <hash>    //find the virtual node on the server corresponding to the hash value";
	    		tip += "\ndht head|pull  //fetch server dht table info";
	    		tip += "\ndht info|list  //show local dht table info";
	    		tip += "\ninfo           //show server dht table info";
	    		tip += "\nexit\n";
	    		break;
    	}
    	
    	return tip;
    }
}

class RunDataNode_Rush extends Thread {
	final String ip;
	final int port;
	final int hashRange;
	public RunDataNode_Rush(String ip, int port, int hashRange) {
		this.ip = ip;
		this.port = port;
		this.hashRange = hashRange;
	}
	
	@Override
	public void run() {
		runDataNode(this.ip, this.port, this.hashRange);
	}
	
	public boolean runDataNode(String ip, int port, int hashRange) {
		boolean success = false;
        try {
        	String dataPortNum = String.valueOf(port);
			Process p = Runtime.getRuntime().exec("java -classpath .:../lib/* dht/rush/DataNode " + ip + " " + dataPortNum + " " + hashRange);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null) {
			    System.out.println(line);
			}
			
			System.out.println("Data Node " + p + " " + (p.isAlive() ? "running " + "at " + ip + ":" + dataPortNum : "at " + ip + ":" + dataPortNum + " not running"));
			
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			success = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return success;
	}
}
