///*
//* storage_server.java modified from JSONRouter.java
//* 
//* Proof of concept JSON Server that listens for requests on port 9000 of the format:
//* 	method - Method name
//* 	to - Name of intended server
//* 	from - Name of client
//* 	params - JSON object containing parameters for method
//* 
//* Returns a response of the form
//* 	response - Always OK, probably should actually do something with request first
//* 	to - Client name
//*	from - Name of intended server
//*
//* 	(c) 2018 Li Jincheng
//*/

package storage_server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class storage_server {
	
	public class RequestRouter extends Thread {
	    private Socket socket;
	    private RequestMap map;
	    
	    public RequestRouter(Socket socket, RequestMap map) {
	        this.socket = socket;
	        this.map = map;
	        System.out.println("Opening request router...");
	    }
	    
	    public void run() {
	        try {
	            BufferedReader in = new BufferedReader(
	                    new InputStreamReader(socket.getInputStream()));

	            // Send a welcome message to the client.
	            System.out.println("Request received...");
	            
	            // Too heavy needs to be refactored into a parseRequest method that returns Request object
	            while (true) {
	                String input = in.readLine();
	                if (input == null || input.equals(".")) {
	                    break;
	                }
	                System.out.println(input);
	                JsonReader jsonReader = Json.createReader(new StringReader(input));
	                JsonObject jobj = jsonReader.readObject();
	                
	                // Parse request
	    			// Adapted from https://javaee.github.io/jsonp/getting-started.html
	                String method = jobj.getString("method");
	                String to = jobj.getString("to");
	                String from = jobj.getString("from");
	                JsonObject params = jobj.getJsonObject("params");
	                
	                System.out.println(to + " received command to " + method + " from " + from + " with parameters " + params.toString());
	                
	                String rlt = map.Execute(method, params);
	                
	                String response = "{\"from\":\"" + to + "\",\"to\":\"" + from + "\", \"response\": \"OK\", \"result\": \"" + rlt + "\"}";
	                socket.getOutputStream().write(response.getBytes());
	            }
	        } catch (IOException e) {
	        	System.out.println("++ Could not read socket ++");
	        } finally {
	            try {
	                socket.close();
	            } catch (IOException e) {
	                log("?? Couldn't close a socket, what's going on ??");
	            }
	            System.out.println("-- Socket closed --");
	        }
	    }

	    /**
	     * Logs a simple message.  In this case we just write the
	     * message to the server applications standard output.
	     */
	    private void log(String message) {
	        System.out.println(message);
	    }
	}

	static class CloudFiles {
	    private static HashMap<String, ReentrantLock> locks;
	    public static String writeFile(String fileName)
	    {
	        if(! locks.containsKey(fileName))
	        {
	            locks.put(fileName, new ReentrantLock());
	        }
	        ReentrantLock lock = locks.get(fileName);
	        if(lock.tryLock())
	        {
	            try{
	                Thread.sleep(1000);
	            }
	            catch (Exception e)
	            {}
	            lock.unlock();
	            System.out.println("write " + fileName + " success;");
	            return "write " + fileName + " success;";
	        }
	        else
	        {
	            System.out.println("write " + fileName + " fail; lock " + fileName + " fail;");
	            return "write " + fileName + " fail; lock " + fileName + " fail;";
	        }
	    }

	    public static String readFile(String fileName)
	    {
	        if(! locks.containsKey(fileName))
	        {
	            System.out.println("read " + fileName + " fail; " + fileName + " not exist;");
	            return "read " + fileName + " fail; " + fileName + " not exist;";
	        }
	        ReentrantLock lock = locks.get(fileName);
	        //do we need read lock?
	        if(lock.tryLock())
	        {
	            try{
	                Thread.sleep(1000);
	            }
	            catch (Exception e)
	            {}
	            lock.unlock();
	            System.out.println("read " + fileName + " success;");
	            return "read " + fileName + " success;";
	        }
	        else
	        {
	            System.out.println("read " + fileName + " fail; lock " + fileName + " fail;");
	            return "read " + fileName + " fail; lock " + fileName + " fail;";
	        }
	    }
	}

	abstract class Method {
		public abstract String run(JsonObject params);
	}

	class WriteMethod extends Method
	{
		@Override
		public String run(JsonObject params)
		{
			System.out.println("WRITE: Params provided were: " + params.toString());
	        return CloudFiles.readFile(params.getString("file"));
		}
	}

	class ReadMethod extends Method
	{
		@Override
		public String run(JsonObject params)
		{
			System.out.println("READ: Params provided were: " + params.toString());
	        return CloudFiles.writeFile(params.getString("file"));
		}
	}

	public class RequestMap {
		HashMap<String, Method> map;
		public RequestMap() {
			map = new HashMap<>();
			map.put("write", new WriteMethod());
			map.put("read", new ReadMethod());
		}
		
		public String Execute(String methodName, JsonObject params)
		{
			Method method = map.get(methodName);
			return method.run(params);
		}
	}
	
	public static void main(String args[]) {
		int PORT = 19001;
		storage_server server = new storage_server();
		storage_server.RequestMap map = server.new RequestMap();
		System.out.println("==== Data Node =====\n");

		// Adapted from http://cs.lmu.edu/~ray/notes/javanetexamples/
		try (ServerSocket listener = new ServerSocket(PORT)){
			System.out.println("Listening on port: " + Integer.toString(PORT));
			while(true) {
				Socket socket = listener.accept();
				server.new RequestRouter(socket, map).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}



