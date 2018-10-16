/**
 * control_client.java
 *
 * DHT1: Cassandra and Swift, DHT ring
 * DHT2: Ceph,rush and crush
 * DHT3: Elastic DHT: Similar to Redis 
 *
 *  (c) 2018 Li Jincheng
 */
//package control_client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.io.Console;
import java.util.Vector;
import java.lang.String;
import java.io.File;
import java.io.FileReader;

interface dht_table
{
    /*
    * Physical node insertion and removal
    * Virtual node insertion and removal
    * Load balance request (see load balance section)
    * Report: Access the data structures and print them to show the correctness of the implementation
     */
    public void insert_physical_node();
    public void remove_physical_node(int node);
    public Vector query_physical_node();
    public void insert_virtual_node();
    public void remove_virtual_node(int node);
    public Vector query_virtual_node();
    public Vector query_loadbalance();
    public void do_loadbalance();
    public String report_datastructure();
}

class DHT_example implements dht_table
{
    /*
    * Physical node insertion and removal
    * Virtual node insertion and removal
    * Load balance request (see load balance section)
    * Report: Access the data structures and print them to show the correctness of the implementation
     */
    public void insert_physical_node()
    {
        System.out.println("Starting send request insert_physical_node");
    }
    public void remove_physical_node(int node)
    {
        System.out.println("Starting send request remove_physical_node");
    }
    public Vector query_physical_node()
    {
        System.out.println("Starting send request query_physical_node");
        return new Vector();
    }
    public void insert_virtual_node()
    {
        System.out.println("Starting send request insert_virtual_node");
    }
    public void remove_virtual_node(int node)
    {
        System.out.println("Starting send request remove_virtual_node");
    }
    public Vector query_virtual_node()
    {
        System.out.println("Starting send request query_virtual_node");
        return new Vector();
    }
    public Vector query_loadbalance()
    {
        System.out.println("Starting send request query_loadbalance");
        return new Vector();
    }
    public void do_loadbalance()
    {
        System.out.println("Starting send request do_loadbalance");
    }
    public String report_datastructure()
    {
        System.out.println("Starting send request report_datastructure");
        return "report haha";
    }
}
public class control_client {
    //Table initialization
    public static dht_table initialize_DHT1()
    {
        //initialize_JSONRouter();
        return new DHT_example();
    }
    public static dht_table initialize_DHT2()
    {
        //initialize_JSONRouter();
        return new DHT_example();
    }
    public static dht_table initialize_DHT3()
    {
        //initialize_JSONRouter();
        return new DHT_example();
    }
    public static void main(String args[])
    {
        System.out.println("==== Welcome to Control Client !!! =====");
        System.out.println("==== There are three types of DHT solutions, please select one(1,2,3) =====\n");
        System.out.println("DHT1: Cassandra and Swift, DHT ring");
        System.out.println("DHT2: Ceph,rush and crush ");
        System.out.println("DHT3: Elastic DHT: Similar to Redis\n");
        Console console = System.console();
        String dht = console.readLine("Please select from: 1 , 2 or 3:");
        dht_table DHT_server = new DHT_example();
        if(dht == "1")
        {
            DHT_server = initialize_DHT1();
        }
        if(dht == "2")
        {
            DHT_server = initialize_DHT2();
        }
        if(dht == "3")
        {
            DHT_server = initialize_DHT3();
        }
        Vector<String> cmds = new Vector<String>();
        while(true)
        {
            if(cmds.isEmpty() == true)
            {
                String cmd = console.readLine("Input your command (help/exit/read file):");
                cmds.addElement(cmd);
            }
            String cmd = cmds.remove(0);
            System.out.println(cmd);
            if(cmd.equals("help"))
            {
                System.out.println("--> insert/remove/query physical/virtual [node]");
                System.out.println("--> query/do loadbalance");
                System.out.println("--> report datastructure");
            }
            else if(cmd.equals("exit"))
            {
                System.exit(0);
            }
            else if(cmd.startsWith("read"))
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
            else
            {
                switch(cmd)
                {
                    case "insert physical node":
                        DHT_server.insert_physical_node();
                        break;
                    case "remove physical node":
                        DHT_server.remove_physical_node(0);
                        break;
                    case "query physical node":
                        DHT_server.query_physical_node();
                        break;
                    case "insert virtual node":
                        DHT_server.insert_virtual_node();
                        break;
                    case "remove virtual node":
                        DHT_server.remove_virtual_node(0);
                        break;
                    case "query virtual node":
                        DHT_server.query_virtual_node();
                        break;
                    case "query loadbalance":
                        DHT_server.query_loadbalance();
                        break;
                    case "do loadbalance":
                        DHT_server.do_loadbalance();
                        break;
                    case "report datastructure":
                        DHT_server.report_datastructure();
                        break;
                }
            }
        }
    }
}
