package com.google.code.gossip.examples;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.io.*;
import java.util.*;

public class StartRing
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
        Element nodes = document.getRootElement().element("nodes");
        List<Element> listOfNodes = nodes.elements();

        int physical_machine = listOfNodes.size();
        int each_machine_physical = phy_nodes_num / physical_machine;
        String shpath = " com/google/code/gossip/examples/GossipRing "; 
        String variables = " -classpath ../../lib/\\*:./ ";
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("commands.sh"), "utf-8"));

        String command1 = "";
        for (int i = 0; i < physical_machine; i++)
        {
            String ip = listOfNodes.get(i).element("ip").getStringValue();
            int port = Integer.parseInt(listOfNodes.get(i).element("port").getStringValue());
            //System.out.println(ip + " " +port);
            for (int j = 0; j < each_machine_physical; j++)
            {
                String backup1 = "", backup2 = "";
                String parametors = ip + " " + (port + j) + " ";
                if(j < each_machine_physical - 2)
                {
                    backup1 = ip + " " + (port + j + 1) + " ";
                    backup2 = ip + " " + (port + j + 2) + " ";
                }
                else if(j == each_machine_physical - 2 && i < physical_machine - 1)
                {
                    String next_ip = listOfNodes.get(i + 1).element("ip").getStringValue();
                    backup1 = ip + " " + (port + j + 1) + " ";
                    backup2 = next_ip + " 0 ";
                }
                else if(j == each_machine_physical - 1 && i < physical_machine - 1)
                {
                    String next_ip = listOfNodes.get(i + 1).element("ip").getStringValue();
                    backup1 = next_ip + " 0 ";
                    backup2 = next_ip + " 1 ";
                }
                else if(j == each_machine_physical - 2 && i == physical_machine - 1)
                {
                    String next_ip = listOfNodes.get(0).element("ip").getStringValue();
                    backup1 = ip + " " + (port + j + 1) + " ";
                    backup2 = next_ip + " 0 ";
                }
                else if(j == each_machine_physical - 1 && i == physical_machine - 1)
                {
                    String next_ip = listOfNodes.get(0).element("ip").getStringValue();
                    backup1 = next_ip + " 0 ";
                    backup2 = next_ip + " 1 ";
                }
                else
                {
                    System.out.println("Unexpected location!!!");
                    System.exit(1);
                }
                if (i == 0 && j == 0)
                    command1 = "cd " + path + " && java " + variables + shpath + parametors;
                else
                {
                    String log = ip + "_" + (port + j) + ".log";
<<<<<<< HEAD
                    String command = "cd " + path + " && java " + variables + shpath + parametors + backup1 + backup2 + " > " + log + " 2>&1 & \n";
=======
                    String command = " cd " + path + " && java " + variables + shpath + parametors + " > " + log + " 2>&1 & \n";
>>>>>>> 03ec29630f1b175a5a1e99a84038fb2843bf3103
                    System.out.println(command);
                    writer.write(command);
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
