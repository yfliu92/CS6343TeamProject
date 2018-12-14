package dht.elastic_DHT_centralized;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import dht.common.Hashing;
import dht.common.response.Response;
import dht.server.Command;

public class client {
	private LookupTable lookupTable;
	public static int hashRange;
	private String proxyIP;
	private int proxyPort;
	
	public void printTable() {
		if (lookupTable == null) {
			System.out.println("Local DHT table is not initialized");
			return;
		}
		lookupTable.print();
	}
	
	public boolean buildTable(JsonObject data) {
		lookupTable = new LookupTable();
		return lookupTable.buildTable(data);
	}
	
	public boolean buildTable(LookupTable lookupTable, JsonObject data) {
		lookupTable = new LookupTable();
		return lookupTable.buildTable(data);
	}
	
	public void printTableInfo() {
		int items = lookupTable != null ? lookupTable.getBucketsTable().size() : 0;
		int physicalNodes = lookupTable != null ? lookupTable.getPhysicalNodesMap().size() : 0;
		String epoch = items != 0 ? String.valueOf(lookupTable.getEpoch()): "";
		System.out.println("A total of " + items + " hash buckets and " + physicalNodes + " physical nodes found in the table, epoch " + epoch);
	}
	
	public boolean isTableLatest(String epoch) {
		String localEpoch = lookupTable != null ? String.valueOf(lookupTable.getEpoch()): "";
		if (localEpoch.equals(epoch)) {
			return true;
		}
		else {
			return false;
		}
	}
	
    public LookupTable getLookupTable() {
        return lookupTable;
    }
	
	public String getDHTEpoch() {
		String epoch = this.lookupTable != null ? String.valueOf(this.lookupTable.getEpoch()) : "";
		return epoch;
	}
	
	public List<String> findNodeInfo(int rawhash) {
		List<String> info = new ArrayList<String>();
		if (this.lookupTable.getBucketsTable() != null && this.lookupTable.getBucketsTable().size() > 0) {
			HashMap<String, String> physicalNodes = this.lookupTable.getBucketsTable().get(rawhash);
			for(HashMap.Entry<String, String> pair: physicalNodes.entrySet()) {
				info.add(pair.getValue().replaceAll("\"", "").replace("\\", "").replace("\\", ""));
				
				System.out.println(pair.getValue().replace("\\", "").replace("\\", ""));
			}
		}
		return info;
	}
	
	public void initialize() {
		String xmlPath = System.getProperty("user.dir") + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";
//      String xmlPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "dht" + File.separator + "elastic_DHT_centralized" + File.separator + "config_ElasticDHT.xml";
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
        this.hashRange = Integer.parseInt(rootElement.element("initial_hash_range").getStringValue());
        Element proxyNode = rootElement.element("proxy");
        this.proxyIP = proxyNode.element("ip").getStringValue();
        this.proxyPort = Integer.parseInt(proxyNode.element("port").getStringValue());
        
	}
	
    public static void main(String[] args) throws Exception {
        System.out.println("==== Welcome to Client !!! =====");
        
    	int dhtType = 3;
    	String dhtName = dhtType == 1 ? "DHT Ring" : dhtType == 2 ? "DHT Ceph" : dhtType == 3 ? "Elastic DHT" : "";
        
        client myclient = new client();
        myclient.initialize();

        if (args.length == 0) {
        	RWClient rwclient = new RWClient();
        	boolean connected = rwclient.connectServer(myclient.proxyIP, myclient.proxyPort, myclient, true);
    		
    		if (connected) {
    			System.out.println("Connected to " + dhtName + " Proxy Server ");
    			try {
					rwclient.processCommand(dhtType, "dht pull");
					rwclient.disconnectServer();
					System.out.println("Disconnected to " + dhtName + " Proxy Server ");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			
    			
    		}
    		else {
    			System.out.println("Unable to connect to " + dhtName + " Proxy Server!");
    			return;
    		}
    		
    		Console console = System.console();
            while(true)
            {
            	String cmd = console.readLine("Input your command:");
                
            	try {
					rwclient.processCommand(dhtType, cmd);
				} catch (Exception e) {
					// TODO Auto-generated catch block
//					e.printStackTrace();
				}
            }
        }
        else {
          String serverAddress = "localhost";
        	int port = 0; 
        	if (args.length >= 2) {
        		serverAddress = args[0];
        		port = Integer.valueOf(args[1]);
        	}
        	
        	RWClient rwclient = new RWClient();
        	boolean connected = rwclient.connectServer(serverAddress, port, myclient, false);
    		
    		if (connected) {
    			System.out.println("Connected to " + dhtName + " Data Node");
    			rwclient.processCommand(dhtType, "dht pull");
    		}
    		else {
    			System.out.println("Unable to connect to " + dhtName + " Data Node!");
    			return;
    		}
    		
    		Console console = System.console();
            while(true)
            {
            	String cmd = console.readLine("Input your command:");
                
            	try {
					rwclient.processCommand(dhtType, cmd);
				} catch (Exception e) {
					// TODO Auto-generated catch block
//					e.printStackTrace();
				}
            }
        }



    }

}

class RWClient {
    PrintWriter output;
    BufferedReader input;
    InputStream inputStream;
    OutputStream outputStream;
	SocketAddress socketAddress;
	Socket socket;
	boolean isProxy;
	client myclient;
	
    public boolean connectServer(String serverAddress, int port, client myclient, boolean isProxy) {
    	int timeout = 2000;
		try {
			socketAddress = new InetSocketAddress(serverAddress, port);
			socket = new Socket();
			socket.connect(socketAddress, timeout);
			inputStream = socket.getInputStream();
			outputStream = socket.getOutputStream();
			output = new PrintWriter(outputStream, true);
			input = new BufferedReader(new InputStreamReader(inputStream));
			this.isProxy = isProxy;
			this.myclient = myclient;

	        System.out.println("Connected to server " + serverAddress + ":" + port + ", with local port " + socket.getLocalPort());
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
    
    public void sendCommandStr(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	String[] jsonCommands = {"read", "write", "data", "dht", "find", "info", "writebatch", "updatebatch"};
    	for(String jsonCommand: jsonCommands) {
    		if (command.getAction().equals(jsonCommand)) {
    			sendCommandStr_JsonRes(command, input, output);
    			return;
    		}
    	}

    	System.out.println("Sending command" + " ---- " + new Date().toString());
            output.println(command);
            output.flush();
        String response = input.readLine();
        System.out.println("Response received: " + response + " ---- " + new Date().toString());
    }
    
    public void sendCommandJson(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	sendCommandJson(command, input, output, false);
    }
    
    public void sendCommandJson(Command command, BufferedReader input, PrintWriter output, boolean skipLocal) throws Exception {
    	if (!skipLocal && parseLocalRequest(command)) {
    		return;
    	}
    	
    	String timeStamp = new Date().toString();
    	System.out.println("Sending command" + " ---- " + timeStamp);
    	
    	output.println(new Response(true, command.getRawCommand(), command.getRawCommand()).serialize());
    	output.flush();
        
        JsonObject res = parseRequest(input);
        if (res != null) {
            System.out.println();
            System.out.println("Response received at " + timeStamp);
            System.out.println(res.toString());
            parseResponse(res, command, input, output);
        	
        	System.out.println();
         }
    }
    
    public void sendCommandStr_JsonRes(Command command, BufferedReader input, PrintWriter output) throws Exception {
    	if (parseLocalRequest(command)) {
    		return;
    	}
    	
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
    
    public boolean parseLocalRequest(Command command) {
    	boolean processed = false;
    	if (command.getAction().equals("dht")) {
    		if (command.getCommandSeries().size() > 0 && command.getCommandSeries().get(0).equals("info")) {
    			System.out.println("Epoch Number of Local DHT is " + this.myclient.getDHTEpoch());
    			processed = true;
    		}
    		else if (command.getCommandSeries().size() > 0 && command.getCommandSeries().get(0).equals("print")) {
    			this.myclient.printTable();
    			processed = true;
    		}
    	}
    	else if ((command.getAction().equals("read") ||command.getAction().equals("write")) && command.getCommandSeries().size() > 0) {
			String dataStr = command.getCommandSeries().get(0);
			
			if (dataStr.equals("auto")) {
				int count = 0;
				while(count < 100) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Random ran = new Random();
					int ranhash = ran.nextInt(this.myclient.hashRange);
					List<String> nodeinfo = this.myclient.findNodeInfo(ranhash);
					String message = dataStr + " (hash value: " + ranhash + ") can be found in Data Node " + Arrays.toString(nodeinfo.toArray());
					System.out.println(message);
					
					for(String node: nodeinfo) {
						try {
							String ip = node.split("-")[0];
							int port = Integer.valueOf(node.split("-")[1]);
							boolean connected = connectServer(ip, port, this.myclient, false);
							if (connected) {
								sendCommandJson(command, this.input, this.output, true);
//								disconnectServer();
							}
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					count++;
				}
			}
			else {
				int rawhash = Hashing.getHashValFromKeyword(dataStr, this.myclient.hashRange);
				try {
					if (rawhash < this.myclient.hashRange) {
						rawhash = Integer.valueOf(dataStr);
					}
				}
				catch (Exception e) {
					
				}
				List<String> nodeinfo = this.myclient.findNodeInfo(rawhash);
				String message = dataStr + " (hash value: " + rawhash + ") can be found in Data Node " + Arrays.toString(nodeinfo.toArray());
				System.out.println(message);
				
				for(String node: nodeinfo) {
					try {
						String ip = node.split("-")[0];
						int port = Integer.valueOf(node.split("-")[1]);
						boolean connected = connectServer(ip, port, this.myclient, false);
						if (connected) {
							sendCommandJson(command, this.input, this.output, true);
//							disconnectServer();
						}
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			

			
			processed = true;
    		
    	}
    	return processed;
    }
    
    public void parseResponse(JsonObject res, Command command, BufferedReader input, PrintWriter output) throws Exception {
    	if (command.getAction().equals("info")) {
    		String message = "Data Epoch number: ";
    		if (res.containsKey("message") ) {
    			message = res.getString("message");
    		}
    		if (res.containsKey("result")) {
    			System.out.println(message + " --- " + res.getString("result"));
    		}
    	}
    	else if (command.getAction().equals("dht")) {
    		if (command.getCommandSeries().size() > 0 && command.getCommandSeries().get(0).equals("pull")) {
    			this.myclient.buildTable(res.getJsonObject("jsonResult"));
    			System.out.println("Local DHT built, with epoch number " + this.myclient.getDHTEpoch());
    		}
    		else if (command.getCommandSeries().size() > 1 && command.getCommandSeries().get(0).equals("head") && command.getCommandSeries().get(1).equals("print")) {
//    			LookupTable lookupTable = new LookupTable();
//    			client newclient = new client();
//    			newclient.buildTable(lookupTable, res.getJsonObject("jsonResult"));
//    			System.out.println("Remote DHT, epoch number " + lookupTable.getEpoch());
//    			lookupTable.print();
//    			newclient.printTable();
    			this.myclient.buildTable(res.getJsonObject("jsonResult"));
    			System.out.println("Local DHT built, with epoch number " + this.myclient.getDHTEpoch());
    			this.myclient.printTable();
    		}
    	}
    	else if (command.getAction().equals("read") || command.getAction().equals("write")) {
    		if (res.containsKey("result")) {
    			long remoteEpoch = Long.parseLong(res.getString("result"));
    			long localEpoch = Long.parseLong(this.myclient.getDHTEpoch());
    			if (remoteEpoch > localEpoch) {
    				sendCommandJson(new Command("dht pull"), input, output, true);
    			}
    			
    		}
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
		if(command.getAction().equals("addnode")) {
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
		else if(command.getAction().equals("deletenode")) {
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
		else if(command.getAction().equals("getnodes")) {
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
			System.out.println("command not supported");
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
        	if (isProxy) {
        		sendCommandStr_JsonRes(command, input, output);
        	}
        	else {
        		sendCommandJson(command, input, output);
        	}
        	
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
	    		tip += "\ndht info|print  //show local dht table info";
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
	    		tip += "\ndht info|print  //show local dht table info";
	    		tip += "\ninfo           //show server dht table info";
	    		tip += "\nexit\n";
	    		break;
    	}
    	
    	return tip;
    }
}
