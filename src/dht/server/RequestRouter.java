package dht.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.Socket;
import java.util.HashMap;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import dht.server.method.*;

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
	                
	                map.Execute(method, params.toString());
	                
	                String response = "{\"from\":\"" + to + "\",\"to\":\"" + from + "\", \"response\": \"OK\"}";
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
