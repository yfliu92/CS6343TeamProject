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
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

import org.dom4j.Element;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Date;
import java.util.List;

public class CentralServer {
    private Cluster root;
    private ClusterStructureMap clusterStructureMap;
    
    public ClusterStructureMap getClusterMap() {
    	return this.clusterStructureMap;
    }

    public static void main(String[] args) {
        CentralServer cs = new CentralServer();
        String rootPath = System.getProperty("user.dir");
//        String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";

        String xmlPath = rootPath + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
        cs.clusterStructureMap = ConfigurationUtil.parseConfig(xmlPath);

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
        try {
            cs.startup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
	public void initializeDataNode(Cluster root) {
        if (root == null) {
        	return;
        }
//        System.out.println("root" + root);
        
        try {
        	pushDHT(root.getIp(), Integer.valueOf(root.getPort()));
        }
        catch (Exception e){
        	
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
	    		t = new RunDataNode_Rush(serverAddress, port);
	    		t.start();
	    		
	    		Thread.sleep(1000);
	    		connected = client.connectServer(serverAddress, port);
	    	}
	    	
	    	Thread.sleep(1000);
			if (connected) {
				
				System.out.println("Connected to Data Node Server at " + serverAddress + " " + port);
				
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
				
//		        Thread.sleep(3000);
//				client.processCommand(1, "dht push");
//				client.processCommand(1, "exit");
//				client.socket.close();
//				System.out.println("Disconnected to Data Node Server at " + serverAddress + " " + port);
				
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
	  
	    public ClientHandler(Socket s, InputStream inputStream, OutputStream outputStream, BufferedReader input, PrintWriter output) {
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
                    	System.out.println("Request received from " + s.getPort() + ": " + msg + " ---- " + new Date().toString());
                    	System.out.println();
                    	
                      if (requestObject != null) {
	                      ServerCommand command = dispatchCommand(requestObject);
	                      command.setInputStream(inputStream);
	                      command.setOutputStream(outputStream);
	                      command.run();
	                  }
                	}
              
	            } catch (IOException e) { 
	            	System.out.println("Connection reset at " + s.getPort() + " ---- " + new Date().toString());
	                e.printStackTrace(); 
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
	
	public void startup() throws IOException {
//		CentralServer server = new CentralServer();
//		proxy.initializeDataNode();
	    int port = 8100;
        ServerSocket ss = new ServerSocket(port); 
        
        System.out.println("Rush server running at " + String.valueOf(port));
          
        while (true)  
        { 
            Socket s = null; 
              
            try 
            {
                s = ss.accept(); 
                  
                System.out.println("A new client is connected : " + s); 
                  
                InputStream inputStream = s.getInputStream();
                OutputStream outputStream = s.getOutputStream();
            	BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
		        PrintWriter output = new PrintWriter(outputStream, true);

//                Thread t = server.new ClientHandler(s, inputStream, outputStream, input, output); 
                Thread t = new ClientHandler(s, inputStream, outputStream, input, output); 

                t.start(); 
                  
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
	}


//    public void startup() throws IOException {
//        int port = 8100;
//        ServerSocket serverSocket = new ServerSocket(port);
//        InputStream inputStream = null;
//        OutputStream outputStream = null;
//        BufferedReader in = null;
//        PrintWriter out = null;
//
//        System.out.println("Rush server running at " + port);
//        while (true) {
//            try {
//                Socket clientSocket = serverSocket.accept();
//                System.out.println("Connection accepted" + " ---- " + new Date().toString());
//
//                inputStream = clientSocket.getInputStream();
//                outputStream = clientSocket.getOutputStream();
//
//                in = new BufferedReader(new InputStreamReader(inputStream));
//                out = new PrintWriter(outputStream, true);
//                String str;
//                JsonObject requestObject = null;
//                while (true && in != null) {
//                    str = in.readLine();
//                    if (str != null) {
//                        requestObject = StreamUtil.parseRequest(str);
//                        if (requestObject != null) {
//                            ServerCommand command = dispatchCommand(requestObject);
//                            command.setInputStream(inputStream);
//                            command.setOutputStream(outputStream);
//                            command.run();
//                        }
//                    } else {
//                        System.out.println("Connection end " + " ---- " + new Date().toString());
//                        break;
//                    }
//                }
//            } catch (Exception e) {
//                System.out.println("Connection exception");
//                StreamUtil.closeSocket(inputStream);
//                e.printStackTrace();
//            }
//        }
//    }

    private ServerCommand dispatchCommand(JsonObject requestObject) throws IOException {
        String method = requestObject.getString("method").toLowerCase();
        ServerCommand serverCommand = null;
        JsonObject params = null;
        if (method.toLowerCase().equals("addnode")) {
                System.out.println("Adding node command");
                serverCommand = new AddNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((AddNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((AddNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((AddNodeCommand) serverCommand).setPort(params.getString("port"));
                ((AddNodeCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((AddNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.toLowerCase().equals("deletenode")) {
                System.out.println("Deleting node command");
                serverCommand = new DeleteNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((DeleteNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((DeleteNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((DeleteNodeCommand) serverCommand).setPort(params.getString("port"));
                ((DeleteNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
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
        }
        else if (method.toLowerCase().equals("write")) {
                System.out.println("Start to write a file into the cluster");
                serverCommand = new WriteCommand();
                params = requestObject.getJsonObject("parameters");
                ((WriteCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((WriteCommand) serverCommand).setFileName(params.getString("fileName"));
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
        }
        else if (method.toLowerCase().equals("dht")) {
            	System.out.println("DHT fetch command");
            	serverCommand = new GetDHTCommand();
            	params = requestObject.getJsonObject("parameters");
            	((GetDHTCommand) serverCommand).setFetchType(params.getString("fetchtype"));
            	((GetDHTCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
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
//		this.address = address;
//		this.port = port;
//    	this.socket = s; 
//    	this.input = input;
//        this.output = output;
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
//			socket.close();
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
			  .add("method", "addNode")
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
	          .add("method", "deleteNode")
	          .add("parameters", params)
	          .build();
		}
		else if(command.getAction().toLowerCase().equals("getnodes")) {
            params = Json.createObjectBuilder()
                    .add("pgid", command.getCommandSeries().get(0))
                    .build();

            jobj = Json.createObjectBuilder()
                    .add("method", "getNodes")
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
	public RunDataNode_Rush(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}
	
	@Override
	public void run() {
		runDataNode(this.ip, this.port);
	}
	
	public boolean runDataNode(String ip, int port) {
		boolean success = false;
        try {
        	String dataPortNum = String.valueOf(port);
			Process p = Runtime.getRuntime().exec("java -classpath .:../lib/* dht/rush/DataNode " + ip + " " + dataPortNum);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null) {
			    System.out.println(line);
			}
			
			System.out.println("Data Node " + p + " " + (p.isAlive() ? "running " + " at " + ip + ":" + dataPortNum : "not running"));
			
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
