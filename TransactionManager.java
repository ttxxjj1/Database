package db.transaction;

import db.core.Node;
import db.core.LogEntry;
import db.core.LogEntryType;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionManager {
    private final Node node;
    private final Map<String, Transaction> activeTransactions;
    private final Map<String, Set<String>> lockTable;
    private final AtomicLong transactionIdCounter;
    private final ScheduledExecutorService deadlockDetector;
    private final ReentrantReadWriteLock globalLock;
    
    private static final long TRANSACTION_TIMEOUT = 30000;
    private static final long DEADLOCK_DETECTION_INTERVAL = 1000;
    
    public TransactionManager(Node node) {
        this.node = node;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.lockTable = new ConcurrentHashMap<>();
        this.transactionIdCounter = new AtomicLong(0);
        this.deadlockDetector = Executors.newScheduledThreadPool(1);
        this.globalLock = new ReentrantReadWriteLock();
        
        deadlockDetector.scheduleAtFixedRate(this::detectDeadlocks, 
            DEADLOCK_DETECTION_INTERVAL, DEADLOCK_DETECTION_INTERVAL, TimeUnit.MILLISECONDS);
    }
    
    public String beginTransaction() {
        String txnId = "txn_" + transactionIdCounter.incrementAndGet();
        Transaction transaction = new Transaction(txnId, System.currentTimeMillis());
        
        activeTransactions.put(txnId, transaction);
        
        if (node.isLeader()) {
            LogEntry entry = new LogEntry(LogEntryType.TRANSACTION_BEGIN, 
                node.getCurrentTerm(), null, null, txnId);
            node.replicate(entry);
        }
        
        return txnId;
    }
    
    public void beginTransaction(String txnId) {
        if (!activeTransactions.containsKey(txnId)) {
            Transaction transaction = new Transaction(txnId, System.currentTimeMillis());
            activeTransactions.put(txnId, transaction);
        }
    }
    
    public CompletableFuture<Void> commitTransaction(String txnId) {
        Transaction transaction = activeTransactions.get(txnId);
        if (transaction == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Transaction not found: " + txnId));
            return future;
        }
        
        if (transaction.getState() != TransactionState.ACTIVE) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Transaction not active: " + txnId));
            return future;
        }
        
        transaction.setState(TransactionState.PREPARING);
        
        return twoPhaseCommit(transaction);
    }
    
    public void commitTransaction(String txnId, boolean fromReplication) {
        Transaction transaction = activeTransactions.get(txnId);
        if (transaction != null) {
            transaction.setState(TransactionState.COMMITTED);
            applyTransactionWrites(transaction);
            releaseLocks(txnId);
            activeTransactions.remove(txnId);
        }
    }
    
    public CompletableFuture<Void> abortTransaction(String txnId) {
        Transaction transaction = activeTransactions.get(txnId);
        if (transaction == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        transaction.setState(TransactionState.ABORTED);
        releaseLocks(txnId);
        activeTransactions.remove(txnId);
        
        if (node.isLeader()) {
            LogEntry entry = new LogEntry(LogEntryType.TRANSACTION_ABORT, 
                node.getCurrentTerm(), null, null, txnId);
            return node.replicate(entry).thenApply(v -> null);
        }
        
        return CompletableFuture.completedFuture(null);
    }
    
    public void abortTransaction(String txnId, boolean fromReplication) {
        abortTransaction(txnId);
    }
    
    private CompletableFuture<Void> twoPhaseCommit(Transaction transaction) {
        return preparePhase(transaction)
            .thenCompose(prepared -> {
                if (prepared) {
                    return commitPhase(transaction);
                } else {
                    return abortTransaction(transaction.getId());
                }
            });
    }
    
    private CompletableFuture<Boolean> preparePhase(Transaction transaction) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        
        try {
            validateTransaction(transaction);
            
            if (node.isLeader()) {
                LogEntry entry = new LogEntry(LogEntryType.TRANSACTION_COMMIT, 
                    node.getCurrentTerm(), null, null, transaction.getId());
                
                node.replicate(entry)
                    .thenAccept(v -> future.complete(true))
                    .exceptionally(ex -> {
                        future.complete(false);
                        return null;
                    });
            } else {
                future.complete(false);
            }
        } catch (Exception e) {
            future.complete(false);
        }
        
        return future;
    }
    
    private CompletableFuture<Void> commitPhase(Transaction transaction) {
        transaction.setState(TransactionState.COMMITTED);
        applyTransactionWrites(transaction);
        releaseLocks(transaction.getId());
        activeTransactions.remove(transaction.getId());
        return CompletableFuture.completedFuture(null);
    }
    
    private void validateTransaction(Transaction transaction) throws TransactionException {
        if (System.currentTimeMillis() - transaction.getStartTime() > TRANSACTION_TIMEOUT) {
            throw new TransactionException("Transaction timeout: " + transaction.getId());
        }
        
        for (String key : transaction.getReadSet()) {
            byte[] currentValue = node.getStorage().get(key);
            byte[] readValue = transaction.getReadSnapshot().get(key);
            
            if (!Arrays.equals(currentValue, readValue)) {
                throw new TransactionException("Read-write conflict detected: " + key);
            }
        }
    }
    
    private void applyTransactionWrites(Transaction transaction) {
        for (Map.Entry<String, byte[]> entry : transaction.getWriteSet().entrySet()) {
            node.getStorage().put(entry.getKey(), entry.getValue());
        }
    }
    
    public CompletableFuture<byte[]> read(String txnId, String key) {
        Transaction transaction = activeTransactions.get(txnId);
        if (transaction == null || transaction.getState() != TransactionState.ACTIVE) {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Invalid transaction: " + txnId));
            return future;
        }
        
        return acquireReadLock(txnId, key)
            .thenApply(locked -> {
                if (!locked) {
                    throw new RuntimeException("Failed to acquire read lock");
                }
                
                byte[] value = transaction.getWriteSet().get(key);
                if (value == null) {
                    value = node.getStorage().get(key);
                    transaction.addToReadSet(key, value);
                }
                
                return value;
            });
    }
    
    public CompletableFuture<Void> write(String txnId, String key, byte[] value) {
        Transaction transaction = activeTransactions.get(txnId);
        if (transaction == null || transaction.getState() != TransactionState.ACTIVE) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Invalid transaction: " + txnId));
            return future;
        }
        
        return acquireWriteLock(txnId, key)
            .thenAccept(locked -> {
                if (!locked) {
                    throw new RuntimeException("Failed to acquire write lock");
                }
                
                transaction.addToWriteSet(key, value);
            });
    }
    
    private CompletableFuture<Boolean> acquireReadLock(String txnId, String key) {
        return CompletableFuture.supplyAsync(() -> {
            globalLock.readLock().lock();
            try {
                Set<String> holders = lockTable.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
                
                if (holders.isEmpty() || !hasWriteLock(key, txnId)) {
                    holders.add(txnId + ":R");
                    return true;
                }
                
                return waitForLock(txnId, key, LockType.READ);
            } finally {
                globalLock.readLock().unlock();
            }
        });
    }
    
    private CompletableFuture<Boolean> acquireWriteLock(String txnId, String key) {
        return CompletableFuture.supplyAsync(() -> {
            globalLock.writeLock().lock();
            try {
                Set<String> holders = lockTable.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
                
                if (holders.isEmpty() || (holders.size() == 1 && holders.contains(txnId + ":R"))) {
                    holders.clear();
                    holders.add(txnId + ":W");
                    return true;
                }
                
                return waitForLock(txnId, key, LockType.WRITE);
            } finally {
                globalLock.writeLock().unlock();
            }
        });
    }
    
    private boolean hasWriteLock(String key, String excludeTxnId) {
        Set<String> holders = lockTable.get(key);
        if (holders == null) return false;
        
        return holders.stream()
            .anyMatch(holder -> holder.endsWith(":W") && !holder.startsWith(excludeTxnId));
    }
    
    private boolean waitForLock(String txnId, String key, LockType lockType) {
        Transaction transaction = activeTransactions.get(txnId);
        if (transaction == null) return false;
        
        WaitingLock waitingLock = new WaitingLock(txnId, key, lockType);
        transaction.addWaitingLock(waitingLock);
        
        try {
            return waitingLock.await(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    private void releaseLocks(String txnId) {
        globalLock.writeLock().lock();
        try {
            Iterator<Map.Entry<String, Set<String>>> iterator = lockTable.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Set<String>> entry = iterator.next();
                Set<String> holders = entry.getValue();
                
                holders.removeIf(holder -> holder.startsWith(txnId + ":"));
                
                if (holders.isEmpty()) {
                    iterator.remove();
                } else {
                    notifyWaiters(entry.getKey());
                }
            }
        } finally {
            globalLock.writeLock().unlock();
        }
    }
    
    private void notifyWaiters(String key) {
        for (Transaction transaction : activeTransactions.values()) {
            transaction.notifyWaiters(key);
        }
    }
    
    private void detectDeadlocks() {
        Map<String, Set<String>> waitGraph = buildWaitGraph();
        Set<String> deadlockedTxns = findCycles(waitGraph);
        
        for (String txnId : deadlockedTxns) {
            abortTransaction(txnId);
        }
    }
    
    private Map<String, Set<String>> buildWaitGraph() {
        Map<String, Set<String>> waitGraph = new HashMap<>();
        
        for (Transaction transaction : activeTransactions.values()) {
            String txnId = transaction.getId();
            waitGraph.put(txnId, new HashSet<>());
            
            for (WaitingLock waitingLock : transaction.getWaitingLocks()) {
                Set<String> holders = lockTable.get(waitingLock.key);
                if (holders != null) {
                    for (String holder : holders) {
                        String holderTxnId = holder.substring(0, holder.indexOf(':'));
                        if (!holderTxnId.equals(txnId)) {
                            waitGraph.get(txnId).add(holderTxnId);
                        }
                    }
                }
            }
        }
        
        return waitGraph;
    }
    
    private Set<String> findCycles(Map<String, Set<String>> graph) {
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();
        Set<String> cycles = new HashSet<>();
        
        for (String node : graph.keySet()) {
            if (!visited.contains(node)) {
                findCyclesUtil(node, graph, visited, recursionStack, cycles);
            }
        }
        
        return cycles;
    }
    
    private boolean findCyclesUtil(String node, Map<String, Set<String>> graph, 
                                   Set<String> visited, Set<String> recursionStack, 
                                   Set<String> cycles) {
        visited.add(node);
        recursionStack.add(node);
        
        Set<String> neighbors = graph.get(node);
        if (neighbors != null) {
            for (String neighbor : neighbors) {
                if (!visited.contains(neighbor)) {
                    if (findCyclesUtil(neighbor, graph, visited, recursionStack, cycles)) {
                        cycles.add(node);
                        return true;
                    }
                } else if (recursionStack.contains(neighbor)) {
                    cycles.add(node);
                    return true;
                }
            }
        }
        
        recursionStack.remove(node);
        return false;
    }
    
    public void cleanup() {
        long currentTime = System.currentTimeMillis();
        List<String> expiredTxns = new ArrayList<>();
        
        for (Transaction transaction : activeTransactions.values()) {
            if (currentTime - transaction.getStartTime() > TRANSACTION_TIMEOUT) {
                expiredTxns.add(transaction.getId());
            }
        }
        
        for (String txnId : expiredTxns) {
            abortTransaction(txnId);
        }
    }
    
    public void shutdown() {
        deadlockDetector.shutdown();
    }
}

enum TransactionState {
    ACTIVE, PREPARING, COMMITTED, ABORTED
}

enum LockType {
    READ, WRITE
}

class Transaction {
    private final String id;
    private final long startTime;
    private volatile TransactionState state;
    private final Map<String, byte[]> readSnapshot;
    private final Map<String, byte[]> writeSet;
    private final Set<String> readSet;
    private final List<WaitingLock> waitingLocks;
    
    Transaction(String id, long startTime) {
        this.id = id;
        this.startTime = startTime;
        this.state = TransactionState.ACTIVE;
        this.readSnapshot = new ConcurrentHashMap<>();
        this.writeSet = new ConcurrentHashMap<>();
        this.readSet = ConcurrentHashMap.newKeySet();
        this.waitingLocks = new ArrayList<>();
    }
    
    public String getId() { return id; }
    public long getStartTime() { return startTime; }
    public TransactionState getState() { return state; }
    public void setState(TransactionState state) { this.state = state; }
    public Map<String, byte[]> getReadSnapshot() { return readSnapshot; }
    public Map<String, byte[]> getWriteSet() { return writeSet; }
    public Set<String> getReadSet() { return readSet; }
    public List<WaitingLock> getWaitingLocks() { return waitingLocks; }
    
    public void addToReadSet(String key, byte[] value) {
        readSet.add(key);
        if (value != null) {
            readSnapshot.put(key, Arrays.copyOf(value, value.length));
        }
    }
    
    public void addToWriteSet(String key, byte[] value) {
        writeSet.put(key, Arrays.copyOf(value, value.length));
    }
    
    public void addWaitingLock(WaitingLock lock) {
        synchronized (waitingLocks) {
            waitingLocks.add(lock);
        }
    }
    
    public void notifyWaiters(String key) {
        synchronized (waitingLocks) {
            waitingLocks.removeIf(lock -> {
                if (lock.key.equals(key)) {
                    lock.signal();
                    return true;
                }
                return false;
            });
        }
    }
}

class WaitingLock {
    final String txnId;
    final String key;
    final LockType type;
    private final CountDownLatch latch;
    
    WaitingLock(String txnId, String key, LockType type) {
        this.txnId = txnId;
        this.key = key;
        this.type = type;
        this.latch = new CountDownLatch(1);
    }
    
    public boolean await(long timeoutMs) throws InterruptedException {
        return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }
    
    public void signal() {
        latch.countDown();
    }
}

class TransactionException extends Exception {
    TransactionException(String message) {
        super(message);
    }
}
