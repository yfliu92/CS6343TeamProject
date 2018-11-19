package com.google.code.gossip.examples;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.lang.String;
import java.util.Random;
import java.util.ArrayList;
public class auto_generate_client_cmd
{
public static String getRandomString(int length)
{
    String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random=new Random();
    StringBuffer sb=new StringBuffer();
    for(int i=0;i<length;i++)
    {
        int number=random.nextInt(62);
        sb.append(str.charAt(number));
    }
    return sb.toString();
 }


public static void main(String[] args) throws IOException
{
    Random random=new Random();
    if(args.length < 2)
    {
        System.out.println("java auto_generate_client_cmd [cmd count] [file name]");
        System.exit(0);
    }
    int cmds_count = Integer.parseInt(args[0]);
    String filename = args[1];
    File file = new File(filename);
    if (!file.exists()) 
    {
        file.createNewFile();
    }
    BufferedWriter writer = null;
    ArrayList<String> file_names = new ArrayList<String>(); 
    try
    {
        writer = new BufferedWriter(new FileWriter(file));
        for(int i=0; i < cmds_count/10; i++)
        {
            int length=random.nextInt(63) + 1;
            String tmp = getRandomString(length);
            file_names.add(tmp);
        }
        for(int i=0; i < file_names.size(); i++)
        {
            writer.write("write " + file_names.get(i) + "\n");
        }
        for(int i=0; i < 5 * file_names.size(); i++)
        {
            int j = random.nextInt(file_names.size()/4);
            writer.write("read " + file_names.get(j) + "\n");
        }
        String[] cmds = {"read","write"};
        for(int i=0; i < 4 * file_names.size(); i++)
        {
            int j = random.nextInt(file_names.size()/4) + file_names.size() * 3 / 4;
            writer.write(cmds[random.nextInt(2)] + " " + file_names.get(j) + "\n");
        }
    }
    catch(IOException e)
    {}
    finally
    {
        if(writer != null)
        {
            try
            {
                writer.close();
            }
            catch (IOException e1)
            {}
        }
    }
}
}
