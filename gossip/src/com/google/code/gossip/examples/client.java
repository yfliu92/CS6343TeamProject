package com.google.code.gossip.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.io.Console;
import java.util.Vector;
import java.lang.String;
import java.io.File;
import java.io.FileReader;
import java.net.*;
import java.io.*;
import javax.json.*;

import javax.swing.JOptionPane;
import java.util.HashMap;

public class client {
    String getFileServerFromCentral(Socket centralServer, String filename)
    {
        try(PrintWriter out = new PrintWriter(centralServer.getOutputStream(), true))
        {
            out.println("query " + filename);
            BufferedReader input = new BufferedReader(new InputStreamReader(centralServer.getInputStream()));
            String answer = input.readLine();
            String timeReceived = new Date().toString();
            System.out.println(timeReceived + " -- response Received: " + answer);
            return answer;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
    String getFileServerFromDistribute()
    {
        return "";
    }
	public static void main(String[] args) throws IOException {
        if(args.length < 3)
        {
            System.out.println("java client [ip] [port] [filename]");
            System.exit(0);
        }
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        String filename = args[2];
        //cache map from filename to server(ip+port), much to less
        HashMap<String, String> cache1 = new HashMap<String, String>();
        //cache map from server to socket, one to one
        HashMap<String, Socket> cache2 = new HashMap<String, Socket>();

        //Socket centralServer = new Socket("localhost", 9090);
        Vector<String> cmds = new Vector<String>();
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
        BufferedReader input;
        PrintWriter output;
        Socket socket;
        try {
            SocketAddress socketAddress = new InetSocketAddress(ip, port);
            socket = new Socket();
            socket.connect(socketAddress, 1000);
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            input = new BufferedReader(new InputStreamReader(inputStream));
            output = new PrintWriter(outputStream, true);
            for(String cmd : cmds)
            {
                String file_name = cmd.split(" ")[1];
                if(!cache1.containsKey(file_name))
                {
                    String sending_message = "Resend:0;find " + file_name;
                    System.out.println("Sending command" + " ---- " + sending_message + " ---- ");
                    output.println(sending_message);
                    output.flush();
                    String response = input.readLine();
                    System.out.println("Response received: " + response + " ---- " + new Date().toString());
                    JsonReader jsonReader = Json.createReader(new StringReader(response));
                    JsonObject jsonObject = jsonReader.readObject();
                    String serverAddress = jsonObject.getJsonObject("jsonResult").get("id").toString().replace("\"", "");
                    //String serverAddress = getFileServerFromCentral(centralServer, filename);
                    //String serverAddress = getFileServerFromDistribute();
                    cache1.put(file_name, serverAddress);
                    if(!cache2.containsKey(serverAddress))
                    {
                        String info[] = serverAddress.split("-");
                        Socket s_tmp = new Socket(info[0], Integer.parseInt(info[1]));
                        cache2.put(serverAddress, s_tmp);
                    }
                }
                Socket s2 = cache2.get(cache1.get(file_name));
                PrintWriter out2 = new PrintWriter(s2.getOutputStream(), true);
                out2.println(cmd);
                BufferedReader input2 = new BufferedReader(new InputStreamReader(s2.getInputStream()));
                String answer = input2.readLine();
                String timeReceived = new Date().toString();
                System.out.println(timeReceived + " -- response Received: " + answer);
                Thread.sleep(1000);
            }
            socket.close();
        } catch (IOException e1) {
                    System.out.println("Connection Failed.");
                        //e1.printStackTrace();
        }
        catch(Exception e){
                        e.printStackTrace();
                    System.exit(0);
        }
	}
}
