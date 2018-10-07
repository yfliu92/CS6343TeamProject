#!/bin/bash

echo "Testing Single Client-Server"
echo "Building Jars"
echo "-- Building Server"
ant -f ../ant/build-server-single.xml
echo "-- Building Client"
ant -f ../ant/build-client-single.xml

echo "Launching server"
java -jar ../build/Single/SingleServer.jar -m single -h 127.0.0.1 -p 8100 > log/single_server_output.log 2>log/single_server_debug.log &

echo "Launching clients"
java -jar ../build/Single/SingleClient.jar -m single -h 127.0.0.1 -p 8100 -i C101 > log/single_client_output.log &
java -jar ../build/Single/SingleClient.jar -m single -h 127.0.0.1 -p 8100 -i C102 >> log/single_client_output.log $
java -jar ../build/Single/SingleClient.jar -m single -h 127.0.0.1 -p 8100 -i C103 >> log/single_client_output.log &
