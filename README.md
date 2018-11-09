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


## compile and run Ring server
-- javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar dht/Ring/ProxyServer.java
-- javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar;/Users/jj/Downloads/dom4j-2.1.1.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java

javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java
javac -cp /Users/jj/Downloads/dom4j-2.1.1.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java

javac -cp ../lib/javax.json-api-1.1.2.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java
javac -cp ../lib/dom4j-2.1.1.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java
javac -classpath ../lib/\* dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java dht/common/response/*.java storage_server/Datum.java
javac -classpath ../lib/\* dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java dht/common/response/*.java storage_server/Datum.java control_client/control_client.java

-- java dht/Ring/ProxyServer
-- java -classpath .:./javax.json-1.0.jar dht/Ring/ProxyServer
-- java -classpath .:/Users/jj/Downloads/javax.json-1.0.jar dht/Ring/ProxyServer

java -classpath .:/Users/jj/Downloads/javax.json-1.0.jar dht/Ring/ProxyServer
java -classpath .:/Users/jj/Downloads/dom4j-2.1.1.jar dht/Ring/ProxyServer
java -classpath .:../lib/\* dht/Ring/ProxyServer


## compile and run Ring Data Node
javac -classpath ../lib/\* control_client/*.java dht/server/Command.java
java -classpath .:../lib/\* control_client/DataNode


## compile and run control client
javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar control_client/control_client.java dht/server/Command.java
javac -classpath ../lib/\* control_client/control_client.java dht/server/Command.java

java control_client/control_client
java -classpath .:../lib/\* control_client/control_client


## compile and run Rush server
-- javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar dht/rush/test.java dht/rush/clusters/*.java dht/rush/utils/*.java
javac -classpath ../lib/\* dht/rush/*.java dht/rush/clusters/*.java dht/rush/commands/*.java dht/rush/utils/*.java -Xlint:unchecked
java -classpath .:../lib/\* dht/rush/CentralServer
java -classpath .:../lib/\* dht/rush/test


## compile and run Elastic DHT server
javac -classpath ../lib/\* dht/elastic_DHT_centralized/*.java dht/server/Command.java
java -classpath .:../lib/\* dht/elastic_DHT_centralized/ProxyServer


