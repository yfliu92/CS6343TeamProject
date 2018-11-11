package dht.rush;

import dht.rush.clusters.Cluster;
import dht.rush.clusters.ClusterStructureMap;
import dht.rush.commands.*;
import dht.rush.utils.ConfigurationUtil;
import dht.rush.utils.GenerateControlClientCommandUtil;
import dht.rush.utils.StreamUtil;

import javax.json.JsonObject;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class CentralServer {
    private Cluster root;
    private ClusterStructureMap clusterStructureMap;

    public static void main(String[] args) {
        CentralServer cs = new CentralServer();
        String rootPath = System.getProperty("user.dir");
//        String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";

        String xmlPath = rootPath + File.separator + "src" + File.separator + "dht" + File.separator + "rush" + File.separator + "ceph_config.xml";
        cs.clusterStructureMap = ConfigurationUtil.parseConfig(xmlPath);

        if (cs.clusterStructureMap == null) {
            System.out.println("Central Server initialization failed");
            System.exit(-1);
        }

        /**
         * Will generate the control client commands, will be executed only one time, after generating the commands, comment the following code
         */
        GenerateControlClientCommandUtil.setMap(cs.clusterStructureMap);
        GenerateControlClientCommandUtil.run();

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
        BufferedReader in = null;
        PrintWriter out = null;

        System.out.println("Rush server running at " + port);
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Connection accepted" + " ---- " + new Date().toString());

                inputStream = clientSocket.getInputStream();
                outputStream = clientSocket.getOutputStream();

                in = new BufferedReader(new InputStreamReader(inputStream));
                out = new PrintWriter(outputStream, true);
                String str;
                JsonObject requestObject = null;
                while (true && in != null) {
                    str = in.readLine();
                    if (str != null) {
                        requestObject = StreamUtil.parseRequest(str);
                        if (requestObject != null) {
                            ServerCommand command = dispatchCommand(requestObject);
                            command.setInputStream(inputStream);
                            command.setOutputStream(outputStream);
                            command.run();
                        }
                    } else {
                        System.out.println("Connection end " + " ---- " + new Date().toString());
                        break;
                    }
                }
            } catch (Exception e) {
                System.out.println("Connection exception");
                StreamUtil.closeSocket(inputStream);
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
                System.out.println("Adding node command");
                serverCommand = new AddNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((AddNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((AddNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((AddNodeCommand) serverCommand).setPort(params.getString("port"));
                ((AddNodeCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((AddNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            case "deletenode":
                System.out.println("Deleting node command");
                serverCommand = new DeleteNodeCommand();
                params = requestObject.getJsonObject("parameters");
                ((DeleteNodeCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((DeleteNodeCommand) serverCommand).setIp(params.getString("ip"));
                ((DeleteNodeCommand) serverCommand).setPort(params.getString("port"));
                ((DeleteNodeCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            case "getnodes":
                System.out.println("Getting node command");
                serverCommand = new GetNodesCommand();
                params = requestObject.getJsonObject("parameters");
                ((GetNodesCommand) serverCommand).setPgid(params.getString("pgid"));
                ((GetNodesCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            case "loadbalancing":
                System.out.println("Start loading balancing in a subcluster");
                serverCommand = new LoadBalancingCommand();
                params = requestObject.getJsonObject("parameters");
                ((LoadBalancingCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((LoadBalancingCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            case "write":
                System.out.println("Start to write a file into the cluster");
                serverCommand = new WriteCommand();
                params = requestObject.getJsonObject("parameters");
                ((WriteCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((WriteCommand) serverCommand).setFileName(params.getString("fileName"));
                break;

            case "read":
                System.out.println("Start to return a physical node for the file");
                serverCommand = new ReadCommand();
                params = requestObject.getJsonObject("parameters");
                ((ReadCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                ((ReadCommand) serverCommand).setFileName(params.getString("fileName"));
                break;

            case "getmap":
                System.out.println("Start to get the most recent tree map");
                serverCommand = new GetMapCommand();
                ((GetMapCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            case "changeweight":
                System.out.println("Change node weight command");
                serverCommand = new ChangeWeightCommand();
                params = requestObject.getJsonObject("parameters");
                ((ChangeWeightCommand) serverCommand).setSubClusterId(params.getString("subClusterId"));
                ((ChangeWeightCommand) serverCommand).setIp(params.getString("ip"));
                ((ChangeWeightCommand) serverCommand).setPort(params.getString("port"));
                ((ChangeWeightCommand) serverCommand).setWeight(Double.parseDouble(params.getString("weight")));
                ((ChangeWeightCommand) serverCommand).setClusterStructureMap(this.clusterStructureMap);
                break;

            default:
                System.out.println("Unknown Request");
        }
        return serverCommand;
    }

}
