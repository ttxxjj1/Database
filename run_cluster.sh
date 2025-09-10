#!/bin/bash

if [ ! -d "build/classes" ]; then
    echo "Please compile first: ./compile.sh"
    exit 1
fi

echo "Starting 3-node database cluster..."

java -cp build/classes db.DatabaseMain node1 8081 localhost:8082 localhost:8083 &
NODE1_PID=$!

sleep 2

java -cp build/classes db.DatabaseMain node2 8082 localhost:8081 localhost:8083 &
NODE2_PID=$!

sleep 2

java -cp build/classes db.DatabaseMain node3 8083 localhost:8081 localhost:8082 &
NODE3_PID=$!

echo "Cluster started with PIDs: $NODE1_PID $NODE2_PID $NODE3_PID"
echo "Connect to any node with: telnet localhost 808X"
echo "Press Ctrl+C to stop cluster"

trap "kill $NODE1_PID $NODE2_PID $NODE3_PID" INT
wait
