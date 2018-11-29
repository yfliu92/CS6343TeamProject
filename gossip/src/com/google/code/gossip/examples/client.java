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
import java.util.*;

import javax.swing.JOptionPane;
import java.util.HashMap;
import com.google.code.gossip.manager.Ring.BinarySearchList;
import com.google.code.gossip.manager.Ring.Hashing;

public class client {
    public static BinarySearchList physicalNodes;

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
        
        BinarySearchList physicalNodes = new BinarySearchList(-1);

        //Socket centralServer = new Socket("localhost", 9090);
        ArrayList<String> cmds = new ArrayList<String>();
        File file = new File(filename);
        BufferedReader reader = null;
        try
        {
                  reader = new BufferedReader(new FileReader(file));
                  String tempString;
                    while ((tempString = reader.readLine()) != null)
                    {
                        System.out.println(tempString);
                        cmds.add(tempString);
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
        Socket socket = new Socket();
        HashMap<SocketAddress, Socket> map = new HashMap<SocketAddress, Socket>();
        try {
        while(true)
        {
            SocketAddress socketAddress = new InetSocketAddress(ip, port);
            socket.connect(socketAddress, 1000);
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
            PrintWriter output = new PrintWriter(outputStream, true);
            String sending_message = "Resend:0;info";
            //read DHT table
            output.println(sending_message);
            output.flush();
            String response = input.readLine();
            System.out.println("Response received: " + response + " ---- " + new Date().toString());
            physicalNodes.updateNode(response);
            Thread.sleep(3000);
            while(cmds.size() != 0)
            {
                Socket socket2 = null;
                String rw_filename = cmds.get(0).split(" ")[1];
                int hash = Hashing.getHashValFromKeyword(rw_filename);
                int index = physicalNodes.find(hash);
                SocketAddress socketAddress2 = new InetSocketAddress(physicalNodes.get(index).getAddress(), physicalNodes.get(index).getPort());
                System.out.println(socketAddress2);
                if(!map.containsKey(socketAddress2))
                {
                    socket2 = new Socket();
                    socket2.connect(socketAddress2, 1000);
                    map.put(socketAddress2, socket2);
                }
                InputStream inputStream2 = map.get(socketAddress2).getInputStream();
                OutputStream outputStream2 = map.get(socketAddress2).getOutputStream();
                BufferedReader input2 = new BufferedReader(new InputStreamReader(inputStream2));
                PrintWriter output2 = new PrintWriter(outputStream2, true);
                String sending_message2 = "Resend:0;" + cmds.get(0);
                System.out.println("Sending command" + " ---- " + sending_message2 + " ---- ");
                output2.println(sending_message2);
                output2.flush();
                String response2 = input2.readLine();
                System.out.println("Response received: " + response2 + " ---- " + new Date().toString());
                if(response2.contains("failure"))
                {
                    break;
                }
                cmds.remove(0);
                Thread.sleep(1000);
            }
            if(cmds.size() == 0)
                break;
        }
        socket.close();
        } catch (IOException e1) {
                    System.out.println("Connection Failed.");
                    e1.printStackTrace();
        }
        catch(Exception e){
                        e.printStackTrace();
                    System.exit(0);
        }
	}
}
