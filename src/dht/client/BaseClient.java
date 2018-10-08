package dht.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;

import dht.common.Configuration;
import dht.common.Context;
import dht.common.FileObject;

public class BaseClient {
	public int randomWrite()
	{
		int key = 0;
		
		Configuration config = Configuration.getInstance();
		Context context = Context.getInstance();
		FileObject randomFile = FileObject.getRandom();
		key = randomFile.key; // Save so we can ask for it later
		try (ByteArrayOutputStream randomWriteRequest = makeWriteRequest(randomFile, config, context))
		{
			// TODO: Implement Routing Here?
			sendRequest(randomWriteRequest, config.getHost(), config.getPort());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return key;
	}
	
	public void readFile(int key)
	{
		Configuration config = Configuration.getInstance();
		Context context = Context.getInstance();
		try (ByteArrayOutputStream readRequest = makeReadRequest(key, config, context))
		{
			// TODO: Implement Routing Here?
			sendRequest(readRequest, config.getHost(), config.getPort());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private ByteArrayOutputStream makeReadRequest(int key, Configuration config, Context context) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonWriter writer = Json.createWriter(baos);
		
		JsonObject params = Json.createObjectBuilder()
				.add("key", key)
				.build();
				
		JsonObject jobj = Json.createObjectBuilder()
				.add("from",config.getNodeId())
				.add("to","D101") // TODO: Needs to incorporate routing
				.add("epoch",context.getEpoch())
				.add("id",System.currentTimeMillis())
				.add("method","read")
				.add("parameters", params)
				.build();
		
		writer.writeObject(jobj);
		writer.close();
		return baos;
	}

	public void sendKill() 
	{
		Configuration config = Configuration.getInstance();
		try (ByteArrayOutputStream killRequest = makeKillRequest(config))
		{
			// TODO: Implement Routing Here?
			sendRequest(killRequest, config.getHost(), config.getPort());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void sendRequest(ByteArrayOutputStream requestOut, String host, int port)
	{
		// https://systembash.com/a-simple-java-tcp-server-and-tcp-client/
		Socket clientSocket;
				try {
					// Possibly different based on file/
					// TODO: Implement routing strategy here
					clientSocket = new Socket(host, port);
					
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					requestOut.writeTo(outToServer);
					outToServer.write("\n".getBytes());
					String response = inFromServer.readLine(); // TODO: Do something more with response
					System.out.println("FROM SERVER: " + response);
					clientSocket.close();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}

	private ByteArrayOutputStream makeWriteRequest(FileObject randomFile, Configuration config, Context context) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonWriter writer = Json.createWriter(baos);
		
		JsonObject params = Json.createObjectBuilder()
				.add("key", randomFile.key)
				.add("filename", randomFile.filename)
				.add("version", randomFile.version)
				.add("size", randomFile.size)
				.build();
				
		JsonObject jobj = Json.createObjectBuilder()
				.add("from",config.getNodeId())
				.add("to","D101")
				.add("epoch",context.getEpoch())
				.add("id",ThreadLocalRandom.current().nextInt(0, 100000 + 1))
				.add("method","write")
				.add("parameters", params)
				.build();
		
		writer.writeObject(jobj);
		writer.close();
		return baos;
	}
	
	private ByteArrayOutputStream makeKillRequest(Configuration config) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonWriter writer = Json.createWriter(baos);
				
		JsonObject jobj = Json.createObjectBuilder()
				.add("from",config.getNodeId())
				.add("to","D101")
				.add("method","kill")
				.build();
		
		writer.writeObject(jobj);
		writer.close();
		return baos;
	}
}
