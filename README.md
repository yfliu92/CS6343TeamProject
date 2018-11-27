# DHT Routing Server

## test
`test` folder includes test_single.sh which builds and runs a single server with three clients writing and reading random files.  In the `test\log` output logs are written.

## version
Latest release v0.1

## server/client test
cd /src   
javac dht/server/server.java
javac dht/client/client.java
javac dht/client/controlclient.java
java dht/server/server   
java dht/client/client find aifanfa
java dht/client/client loadbalance 3 6
java dht/client/controlclient


## compile and run Ring server/ Data Node / Client
javac -classpath ../lib/\* dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java dht/common/response/*.java storage_server/Datum.java control_client/control_client.java

java -classpath .:../lib/\* dht/Ring/ProxyServer
java -classpath .:../lib/\* dht/Ring/DataNode
java -classpath .:../lib/\* dht/Ring/client


## compile and run control client
javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar control_client/control_client.java dht/server/Command.java
javac -classpath ../lib/\* control_client/*.java dht/server/Command.java dht/common/Hashing.java
java -classpath .:../lib/\* control_client/control_client
java -classpath .:../lib/\* control_client/client


## compile and run Rush server
-- javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar dht/rush/test.java dht/rush/clusters/*.java dht/rush/utils/*.java

javac -classpath ../lib/\* dht/rush/*.java dht/rush/clusters/*.java dht/rush/commands/*.java dht/common/response/*.java storage_server/Datum.java dht/common/Hashing.java dht/rush/utils/*.java dht/server/Command.java -Xlint:unchecked
java -classpath .:../lib/\* dht/rush/CentralServer
java -classpath .:../lib/\* dht/rush/test


## compile and run Elastic DHT server
javac -classpath ../lib/\* dht/elastic_DHT_centralized/*.java dht/server/Command.java dht/common/Hashing.java dht/common/response/Response.java
java -classpath .:../lib/\* dht/elastic_DHT_centralized/ProxyServer

## compile and run Data Node and test client
## Data node should already be compiled with each proxy server
--javac -classpath ../lib/\* control_client/*.java dht/server/Command.java dht/Ring/*.java dht/common/Hashing.java dht/common/response/*.java storage_server/Datum.java
--java -classpath .:../lib/\* control_client/DataNode

java -classpath .:../lib/\* dht/Ring/DataNode <IP> <port>
java -classpath .:../lib/\* dht/elastic_DHT_centralized/DataNode <IP> <port>
java -classpath .:../lib/\* dht/rush/DataNode <IP> <port>

java -classpath .:../lib/\* dht/Ring/client <IP> <port> <dhtType>
java -classpath .:../lib/\* dht/elastic_DHT_centralized/client <IP> <port> <dhtType>
java -classpath .:../lib/\* dht/rush/client <IP> <port> <dhtType>


