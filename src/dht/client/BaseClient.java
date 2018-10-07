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
	public void randomWrite()
	{
		Configuration config = Configuration.getInstance();
		Context context = Context.getInstance();
		
		// https://systembash.com/a-simple-java-tcp-server-and-tcp-client/
		Socket clientSocket;
		try {
			FileObject randomFile = FileObject.getRandom();
			
			// Possibly different based on file
			clientSocket = new Socket(config.getHost(), config.getPort());
			
			DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
			BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			
			ByteArrayOutputStream randomWriteRequest = makeWriteRequest(randomFile, config, context);
			
			randomWriteRequest.writeTo(outToServer);
			outToServer.write("\n".getBytes());
			randomWriteRequest.close();
			String response = inFromServer.readLine();
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
}
