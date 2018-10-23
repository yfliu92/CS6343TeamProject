//package dht.client;

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
        //cache map from filename to server(ip+port), much to less
        HashMap<String, String> cache1 = new HashMap<String, String>();
        //cache map from server to socket, one to one
        HashMap<String, Socket> cache2 = new HashMap<String, Socket>();

        //Socket centralServer = new Socket("localhost", 9090);
        Console console = System.console();
        Vector<String> cmds = new Vector<String>();
        while(true)
        {   
            if(cmds.isEmpty() == true)
            {   
                String cmd = console.readLine("Input your command (exit/write file/read file/get cmd_file):");
                cmds.addElement(cmd);
            }   
            String cmd = cmds.remove(0);
            System.out.println(cmd);
            if(cmd.equals("exit"))
            {   
                System.exit(0);
            }   
            else if(cmd.startsWith("get"))
            {   
                String filename = cmd.split(" ")[1];
                System.out.println(filename);
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
            }
            else if(cmd.startsWith("read") || cmd.startsWith("write"))
            {
                String filename = cmd.split(" ")[1];
                if(!cache1.containsKey(filename))
                {
                    //String serverAddress = getFileServerFromCentral(centralServer, filename);
                    //String serverAddress = getFileServerFromDistribute();
		            String serverAddress = "localhost 9090";
                    cache1.put(filename, serverAddress);
                    if(!cache2.containsKey(serverAddress))
                    {
                        String info[] = serverAddress.split(" ");
                        Socket s_tmp = new Socket(info[0], Integer.parseInt(info[1]));
                        cache2.put(serverAddress, s_tmp);
                    }
                }
                Socket s = cache2.get(cache1.get(filename));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println(cmd);
                BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String answer = input.readLine();
                String timeReceived = new Date().toString();
                System.out.println(timeReceived + " -- response Received: " + answer);
            }
            else
            {
                System.out.println("Unknow Commands");
            }
        }
	}
}
