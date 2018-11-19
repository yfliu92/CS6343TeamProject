package com.google.code.gossip.examples;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.lang.String;
import java.util.Random;
import java.util.ArrayList;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
public class auto_generate_cc_cmd
{

public static void main(String[] args) throws IOException
{
    Random random=new Random();
    if(args.length < 2)
    {
        System.out.println("java auto_generate_cc_cmd [cmd count] [file name]");
        System.exit(0);
    }
    try
    {
    String path = System.getProperty("user.dir");
    String xmlPath = path + "/com/google/code/gossip/manager/Ring/config_ring.xml";
    File inputFile = new File(xmlPath);
    SAXReader reader = new SAXReader();
    Document document = reader.read(inputFile);
    int phy_nodes_num = Integer.parseInt(document.getRootElement().element("physical").getStringValue());
    int cmds_count = Integer.parseInt(args[0]);
    String filename = args[1];
    File file = new File(filename);
    if (!file.exists()) 
    {
        file.createNewFile();
    }
    BufferedWriter writer = null;
        writer = new BufferedWriter(new FileWriter(file));
        for(int i=0; i < cmds_count/10; i++)
        {
            int port = 16000 + i;
            writer.write("add 192.168.0.1 " + port + "\n");
            phy_nodes_num += 1;
        }
        for(int i=0; i < cmds_count*2/5; i++)
        {
            int node = random.nextInt(phy_nodes_num);
            writer.write("balance " + node + "\n");
        }
        for(int i=0; i < cmds_count/10; i++)
        {
            int node = random.nextInt(phy_nodes_num);
            writer.write("remove " + node + "\n");
            phy_nodes_num -= 1;
        }
        for(int i=0; i < cmds_count*2/5; i++)
        {
            int node = random.nextInt(phy_nodes_num);
            writer.write("balance " + node + "\n");
        }
        writer.close();
    }
    catch(Exception e)
    {}
    finally
    {
    }
}
}
