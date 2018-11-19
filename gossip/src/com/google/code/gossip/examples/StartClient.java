package com.google.code.gossip.examples;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.io.*;
import java.util.*;

public class StartClient
{
    public static void main(String[] args) 
    {
        try{
        String path = System.getProperty("user.dir");
        String xmlPath = path + "/com/google/code/gossip/manager/Ring/config_ring.xml";
        System.out.println(xmlPath);
        File inputFile = new File(xmlPath);
        SAXReader reader = new SAXReader();
        Document document = reader.read(inputFile);

        // Read the elements in the configuration file
        int phy_nodes_num = Integer.parseInt(document.getRootElement().element("physical").getStringValue());
        int client = Integer.parseInt(document.getRootElement().element("client").getStringValue());
        Element nodes = document.getRootElement().element("nodes");
        List<Element> listOfNodes = nodes.elements();

        int physical_machine = listOfNodes.size();
        int each_machine_physical = phy_nodes_num / physical_machine;
        int each_physical_client = client / phy_nodes_num;
        String shpath = " com/google/code/gossip/examples/client "; 
        String variables = " -classpath ../../lib/\\*:./ ";
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("client_commands.sh"), "utf-8"));

        String command1 = "";
        for (int i = 0; i < physical_machine; i++)
        {
            String ip = listOfNodes.get(i).element("ip").getStringValue();
            int port = Integer.parseInt(listOfNodes.get(i).element("port").getStringValue());
            //System.out.println(ip + " " +port);
            for (int j = 0; j < each_machine_physical; j++)
            {
                String parametors = ip + " " + (port + j) + " cmds";
		for ( int k =0; k < each_physical_client; k++)
		{
                	if (i == 0 && j == 0 && k == 0)
                    		command1 = "cd " + path + " && java " + variables + shpath + parametors;
                	else
                	{
                    		String log = ip + "_" + (port + j) + ".log";
                    		String command = "ssh root@" + ip + " cd " + path + " && java " + variables + shpath + parametors + " > " + log + " 2>&1 & \n";
                    		System.out.println(command);
                    		writer.write(command);
                	}
		}
            }
        }
        writer.write(command1);
        writer.close();
        }
        catch(Exception e) {
            System.out.println("Failed to start");
            e.printStackTrace();
        }
    }
}
