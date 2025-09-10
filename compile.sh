#!/bin/bash

mkdir -p build/classes

javac -d build/classes -cp . \
    db/core/Node.java \
    db/consensus/RaftState.java \
    db/storage/StorageEngine.java \
    db/storage/BPlusTree.java \
    db/transaction/TransactionManager.java \
    db/network/NetworkHandler.java \
    db/query/QueryProcessor.java \
    db/DatabaseMain.java

if [ $? -eq 0 ]; then
    echo "Compilation successful"
    echo "To run: java -cp build/classes db.DatabaseMain <nodeId> <port> [peers...]"
    echo "Example: java -cp build/classes db.DatabaseMain node1 8081"
    echo "Example: java -cp build/classes db.DatabaseMain node2 8082 localhost:8081"
else
    echo "Compilation failed"
fi
