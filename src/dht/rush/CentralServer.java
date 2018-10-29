package dht.rush;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;
import dht.rush.clusters.Root;
import dht.rush.commands.AddNodeCommand;
import dht.rush.commands.DeleteNodeCommand;
import dht.rush.commands.GetNodesCommand;
import dht.rush.commands.ServerCommand;
import dht.rush.utils.ConfigurationUtil;
import dht.rush.utils.StreamUtil;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class CentralServer {
    private Cluster root;
    private ClusterStructureMap clusterStructureMap;

    public static void main(String[] args) {
        CentralServer cs = new CentralServer();
        String rootPath = System.getProperty("user.dir");
        String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
        cs.clusterStructureMap = ConfigurationUtil.parseConfig(xmlPath, cs.root);

        if (cs.clusterStructureMap == null) {
            System.out.println("Central Server initialization failed");
            System.exit(-1);
        }
        cs.root = cs.clusterStructureMap.getChildrenList().get("R");
        try {
            cs.startup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void startup() throws IOException {
        int port = 8100;
        ServerSocket serverSocket = new ServerSocket(port);
        InputStream inputStream = null;
        OutputStream outputStream = null;

        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                inputStream = clientSocket.getInputStream();
                outputStream = clientSocket.getOutputStream();
                JsonObject requestObject = StreamUtil.parseRequest(inputStream);
                ServerCommand command = dispatchCommand(requestObject);
                command.setInputStream(inputStream);
                command.setOutputStream(outputStream);
                command.run();
                StreamUtil.closeSocket(inputStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private ServerCommand dispatchCommand(JsonObject requestObject) throws IOException {
        String method = requestObject.getString("method");
        ServerCommand serverCommand = null;
        JsonObject params = null;
        switch (method.toLowerCase()) {
            case "addnode":
                serverCommand = new AddNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((AddNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((AddNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((AddNodeCommand) serverCommand).setPort(params.getString("port"));
                ((AddNodeCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((AddNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;
            case "deletenode":
                serverCommand = new DeleteNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((DeleteNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((DeleteNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((DeleteNodeCommand) serverCommand).setPort(params.getString("port"));
                ((DeleteNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            case "getnodes":
                serverCommand = new GetNodesCommand();
                params = requestObject.getJsonObject("parameters");
                ((GetNodesCommand) serverCommand).setPgid(params.getString("pgid"));
                ((GetNodesCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;
            default:
                System.out.println("Unknown Request");
        }
        return serverCommand;
    }

}
