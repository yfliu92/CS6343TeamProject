package dht.rush;

import dht.rush.clusters.Cluster;
import dht.rush.utils.StreamUtil;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;

public class test {
    static int port = 8100;
    static String address = "localhost";

    public static void main(String[] args) {

        try {
            Socket socket = new Socket();
            SocketAddress addr = new InetSocketAddress(address, port);
            socket.connect(addr);

            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            JsonWriter writer = Json.createWriter(baos);
            /******aadNode******/

//            JsonObject params = Json.createObjectBuilder()
//                    .add("subClusterId", "S0")
//                    .add("ip", "testip")
//                    .add("port", "testPort")
//                    .add("weight", "1.0")
//                    .build();
//
//            JsonObject jobj = Json.createObjectBuilder()
//                    .add("method", "addNode")
//                    .add("parameters", params)
//                    .build();

            /********deleteNode************/
//            JsonObject params = Json.createObjectBuilder()
//                    .add("subClusterId", "S0")
//                    .add("ip", "192.168.0.201")
//                    .add("port", "8100")
//                    .build();
//
//            JsonObject jobj = Json.createObjectBuilder()
//                    .add("method", "deleteNode")
//                    .add("parameters", params)
//                    .build();

            JsonObject params = Json.createObjectBuilder()
                    .add("pgid", "PG1")
                    .build();

            JsonObject jobj = Json.createObjectBuilder()
                    .add("method", "getNodes")
                    .add("parameters", params)
                    .build();


            writer.writeObject(jobj);
            writer.close();
            baos.writeTo(outputStream);

            outputStream.write("\n".getBytes());
            outputStream.flush();

            JsonObject res = StreamUtil.parseRequest(inputStream);
            System.out.println(res.toString());
//            System.out.println("STATUS: " + res.getString("status") + ", " + "message: " + res.getString("message"));
            inputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
