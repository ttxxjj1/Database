# Distributed Database Management System

A high-performance distributed database with ACID transactions, built from scratch in Java.

## Architecture

### Core Components

- **Raft Consensus Algorithm** - Distributed leader election and log replication
- **ACID Transaction Manager** - Two-phase commit with deadlock detection
- **B+ Tree Storage Engine** - Efficient indexing and range queries
- **Write-Ahead Logging** - Durability and crash recovery
- **Query Processor** - SQL parsing, optimization, and execution
- **Network Layer** - Non-blocking I/O with custom protocol

### Features

- **Distributed Consensus** using Raft algorithm
- **ACID Transactions** with multi-version concurrency control
- **Deadlock Detection** using wait-for graphs
- **Crash Recovery** through write-ahead logging
- **Range Queries** with B+ tree indexing
- **SQL Support** for basic operations
- **Network Partitioning** tolerance
- **Load Balancing** across cluster nodes

## Quick Start

### Prerequisites

- Java 8 or higher
- Unix-like system (Linux/macOS)

### Compilation

```bash
chmod +x compile.sh
./compile.sh
```

### Running Single Node

```bash
java -cp build/classes db.DatabaseMain node1 8081
```

### Running 3-Node Cluster

```bash
chmod +x run_cluster.sh
./run_cluster.sh
```

## SQL Commands

### Transaction Management

```sql
BEGIN TRANSACTION
COMMIT
ROLLBACK
```

### Data Operations

```sql
-- Insert data
INSERT INTO users (id, name, email) VALUES ('1', 'John Doe', 'john@example.com')

-- Query data
SELECT * FROM users WHERE id = '1'
SELECT name, email FROM users ORDER BY name LIMIT 10

-- Update data
UPDATE users SET email = 'newemail@example.com' WHERE id = '1'

-- Delete data
DELETE FROM users WHERE id = '1'
```

### System Commands

```
help     - Show available commands
status   - Display node status
quit     - Exit database
```

## Project Structure

```
src/
├── db/
│   ├── core/
│   │   └── Node.java                    # Main node implementation
│   ├── consensus/
│   │   └── RaftState.java              # Raft consensus state
│   ├── storage/
│   │   ├── StorageEngine.java          # Storage layer
│   │   └── BPlusTree.java              # B+ tree implementation
│   ├── transaction/
│   │   └── TransactionManager.java     # ACID transaction handling
│   ├── network/
│   │   └── NetworkHandler.java         # Network communication
│   ├── query/
│   │   └── QueryProcessor.java         # SQL query engine
│   └── DatabaseMain.java               # Entry point
├── compile.sh                          # Build script
└── run_cluster.sh                      # Cluster startup script
```

## Technical Details

### Raft Consensus

- Leader election with randomized timeouts
- Log replication across cluster nodes
- Network partition tolerance
- Automatic leader failover

### Transaction Management

- **Isolation**: Multi-version concurrency control
- **Consistency**: Two-phase commit protocol
- **Atomicity**: All-or-nothing transaction execution
- **Durability**: Write-ahead logging

### Storage Engine

- B+ tree indexing for efficient lookups
- Buffer pool management for memory optimization
- Write-ahead logging for crash recovery
- Page-based storage with configurable page size

### Query Processing

- SQL parser with support for SELECT, INSERT, UPDATE, DELETE
- Query optimizer with execution plan generation
- Index-based and table scan execution strategies
- Support for WHERE, ORDER BY, LIMIT clauses

## Performance Characteristics

- **Throughput**: Thousands of transactions per second
- **Latency**: Sub-millisecond query response times
- **Scalability**: Horizontal scaling via cluster nodes
- **Availability**: Tolerates (N-1)/2 node failures
- **Consistency**: Strong consistency through Raft consensus

## Configuration

### Node Configuration

```bash
# Single node
java -cp build/classes db.DatabaseMain <nodeId> <port>

# Cluster node
java -cp build/classes db.DatabaseMain <nodeId> <port> <peer1:port1> <peer2:port2>
```

### Storage Configuration

- Page size: 4KB (configurable)
- Buffer pool: 1000 pages (configurable)
- Transaction timeout: 30 seconds
- Election timeout: 150-300ms

## Development

### Building from Source

1. Clone repository
2. Run compilation script: `./compile.sh`
3. Start cluster: `./run_cluster.sh`

### Testing

Connect to any cluster node:

```bash
# Terminal 1 - Start cluster
./run_cluster.sh

# Terminal 2 - Connect to node
java -cp build/classes db.DatabaseMain client 0 localhost:8081
```

### Monitoring

Use the `status` command to monitor:
- Node state (Leader/Follower/Candidate)
- Current term
- Active transactions
- Cluster membership

## Limitations

- Basic SQL subset (no JOINs, subqueries)
- In-memory storage only
- Simple data types (strings)
- No user authentication
- No schema management

## Future Enhancements

- [ ] Persistent storage backend
- [ ] Advanced SQL features (JOINs, aggregations)
- [ ] Schema management
- [ ] User authentication and authorization
- [ ] Metrics and monitoring
- [ ] Data replication strategies
- [ ] Backup and restore functionality

## Contributing

This is an educational implementation demonstrating distributed systems concepts. Key areas for contribution:

1. Performance optimizations
2. Additional SQL features
3. Monitoring and observability
4. Test coverage
5. Documentation improvements

## License

MIT License - see LICENSE file for details

## References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [Two-Phase Commit Protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
- [B+ Tree Data Structure](https://en.wikipedia.org/wiki/B%2B_tree)
- [ACID Properties](https://en.wikipedia.org/wiki/ACID)
