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