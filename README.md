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

javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar dht/Ring/ProxyServer.java

javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java
javac -cp ../lib/javax.json-api-1.1.2.jar dht/Ring/*.java dht/server/Command.java dht/common/Hashing.java

java dht/Ring/ProxyServer

java -classpath .:./javax.json-1.0.jar dht/Ring/ProxyServer
java -classpath .:/Users/jj/Downloads/javax.json-1.0.jar dht/Ring/ProxyServer

java -cp target/classes:/Users/jj/Downloads/javax.json-api-1.0.jar dht/Ring/ProxyServer

javac -cp /Users/jj/Downloads/javax.json-api-1.0.jar control_client/control_client.java dht/server/Command.java

java control_client/control_client
