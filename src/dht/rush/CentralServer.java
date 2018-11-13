package dht.rush;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;
import dht.rush.commands.*;
import dht.rush.utils.ConfigurationUtil;
import dht.rush.utils.GenerateControlClientCommandUtil;
import dht.rush.utils.StreamUtil;

import javax.json.JsonObject;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class CentralServer {
    private Cluster root;
    private ClusterStructureMap clusterStructureMap;

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
        try {
            cs.startup();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        if (method.equals("addnode")) {
                System.out.println("Adding node command");
                serverCommand = new AddNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((AddNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((AddNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((AddNodeCommand) serverCommand).setPort(params.getString("port"));
                ((AddNodeCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((AddNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.equals("deletenode")) {
                System.out.println("Deleting node command");
                serverCommand = new DeleteNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((DeleteNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((DeleteNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((DeleteNodeCommand) serverCommand).setPort(params.getString("port"));
                ((DeleteNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.equals("getnodes")) {
                System.out.println("Getting node command");
                serverCommand = new GetNodesCommand();
                params = requestObject.getJsonObject("parameters");
                ((GetNodesCommand) serverCommand).setPgid(params.getString("pgid"));
                ((GetNodesCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.equals("loadbalancing")) {
                System.out.println("Start loading balancing in a subcluster");
                serverCommand = new LoadBalancingCommand();
                params = requestObject.getJsonObject("parameters");
                ((LoadBalancingCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((LoadBalancingCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.equals("write")) {
                System.out.println("Start to write a file into the cluster");
                serverCommand = new WriteCommand();
                params = requestObject.getJsonObject("parameters");
                ((WriteCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((WriteCommand) serverCommand).setFileName(params.getString("fileName"));
        }
        else if (method.equals("read")) {
                System.out.println("Start to return a physical node for the file");
                serverCommand = new ReadCommand();
                params = requestObject.getJsonObject("parameters");
                ((ReadCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((ReadCommand) serverCommand).setFileName(params.getString("fileName"));
        }
        else if (method.equals("getmap")) {
                System.out.println("Start to get the most recent tree map");
                serverCommand = new GetMapCommand();
                ((GetMapCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.equals("changeweight")) {
                System.out.println("Change node weight command");
                serverCommand = new ChangeWeightCommand();
                params = requestObject.getJsonObject("parameters");
                ((ChangeWeightCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((ChangeWeightCommand) serverCommand).setIp(params.getString("ip"));
                ((ChangeWeightCommand) serverCommand).setPort(params.getString("port"));
                ((ChangeWeightCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((ChangeWeightCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
        }
        else if (method.equals("dht")) {
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
