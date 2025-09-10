package db.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageEngine {
    private final BPlusTree index;
    private final WriteAheadLog wal;
    private final BufferPool bufferPool;
    private final Map<String, byte[]> memTable;
    private final ReadWriteLock tableLock;
    private final String dataDirectory;
    
    private static final int PAGE_SIZE = 4096;
    private static final int BUFFER_POOL_SIZE = 1000;
    
    public StorageEngine() {
        this.dataDirectory = "data/";
        this.index = new BPlusTree(16);
        this.wal = new WriteAheadLog(dataDirectory + "wal.log");
        this.bufferPool = new BufferPool(BUFFER_POOL_SIZE, PAGE_SIZE);
        this.memTable = new ConcurrentHashMap<>();
        this.tableLock = new ReentrantReadWriteLock();
        
        createDirectories();
    }
    
    private void createDirectories() {
        try {
            Files.createDirectories(Paths.get(dataDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create data directory", e);
        }
    }
    
    public void initialize() {
        try {
            wal.initialize();
            loadFromWAL();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage engine", e);
        }
    }
    
    private void loadFromWAL() throws IOException {
        List<WALEntry> entries = wal.readAll();
        for (WALEntry entry : entries) {
            switch (entry.operation) {
                case PUT:
                    memTable.put(entry.key, entry.value);
                    index.put(entry.key, new DataPointer(entry.key, entry.value.length));
                    break;
                case DELETE:
                    memTable.remove(entry.key);
                    index.delete(entry.key);
                    break;
            }
        }
    }
    
    public void put(String key, byte[] value) {
        tableLock.writeLock().lock();
        try {
            WALEntry entry = new WALEntry(WALOperation.PUT, key, value);
            wal.append(entry);
            
            memTable.put(key, value);
            index.put(key, new DataPointer(key, value.length));
            
            if (memTable.size() > 10000) {
                flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to put key: " + key, e);
        } finally {
            tableLock.writeLock().unlock();
        }
    }
    
    public byte[] get(String key) {
        tableLock.readLock().lock();
        try {
            byte[] value = memTable.get(key);
            if (value != null) {
                return value;
            }
            
            DataPointer pointer = index.get(key);
            if (pointer != null) {
                return loadFromDisk(pointer);
            }
            
            return null;
        } finally {
            tableLock.readLock().unlock();
        }
    }
    
    public void delete(String key) {
        tableLock.writeLock().lock();
        try {
            WALEntry entry = new WALEntry(WALOperation.DELETE, key, null);
            wal.append(entry);
            
            memTable.remove(key);
            index.delete(key);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete key: " + key, e);
        } finally {
            tableLock.writeLock().unlock();
        }
    }
    
    public List<String> scan(String startKey, String endKey, int limit) {
        tableLock.readLock().lock();
        try {
            return index.range(startKey, endKey, limit);
        } finally {
            tableLock.readLock().unlock();
        }
    }
    
    private byte[] loadFromDisk(DataPointer pointer) {
        try {
            Path filePath = Paths.get(dataDirectory, pointer.fileName);
            if (!Files.exists(filePath)) {
                return null;
            }
            
            try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
                file.seek(pointer.offset);
                byte[] data = new byte[pointer.length];
                file.readFully(data);
                return data;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load from disk", e);
        }
    }
    
    private void flush() {
        try {
            String fileName = "data_" + System.currentTimeMillis() + ".dat";
            Path filePath = Paths.get(dataDirectory, fileName);
            
            try (FileChannel channel = FileChannel.open(filePath, 
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                long offset = 0;
                
                for (Map.Entry<String, byte[]> entry : memTable.entrySet()) {
                    byte[] keyBytes = entry.getKey().getBytes();
                    byte[] valueBytes = entry.getValue();
                    
                    int recordSize = 4 + keyBytes.length + 4 + valueBytes.length;
                    
                    if (buffer.remaining() < recordSize) {
                        buffer.flip();
                        channel.write(buffer);
                        offset += buffer.limit();
                        buffer.clear();
                    }
                    
                    buffer.putInt(keyBytes.length);
                    buffer.put(keyBytes);
                    buffer.putInt(valueBytes.length);
                    buffer.put(valueBytes);
                    
                    index.put(entry.getKey(), new DataPointer(fileName, offset, valueBytes.length));
                }
                
                if (buffer.position() > 0) {
                    buffer.flip();
                    channel.write(buffer);
                }
            }
            
            memTable.clear();
            wal.truncate();
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush memtable", e);
        }
    }
    
    public void close() {
        tableLock.writeLock().lock();
        try {
            flush();
            wal.close();
        } finally {
            tableLock.writeLock().unlock();
        }
    }
}

enum WALOperation {
    PUT, DELETE
}

class WALEntry {
    final WALOperation operation;
    final String key;
    final byte[] value;
    final long timestamp;
    
    WALEntry(WALOperation operation, String key, byte[] value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }
}

class WriteAheadLog {
    private final String fileName;
    private FileChannel channel;
    
    WriteAheadLog(String fileName) {
        this.fileName = fileName;
    }
    
    void initialize() throws IOException {
        Path path = Paths.get(fileName);
        channel = FileChannel.open(path, StandardOpenOption.CREATE, 
                                 StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }
    
    void append(WALEntry entry) throws IOException {
        ByteBuffer buffer = serializeEntry(entry);
        buffer.flip();
        channel.write(buffer);
        channel.force(true);
    }
    
    private ByteBuffer serializeEntry(WALEntry entry) {
        byte[] keyBytes = entry.key.getBytes();
        byte[] valueBytes = entry.value != null ? entry.value : new byte[0];
        
        ByteBuffer buffer = ByteBuffer.allocate(
            1 + 8 + 4 + keyBytes.length + 4 + valueBytes.length);
        
        buffer.put((byte) entry.operation.ordinal());
        buffer.putLong(entry.timestamp);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
        
        return buffer;
    }
    
    List<WALEntry> readAll() throws IOException {
        List<WALEntry> entries = new ArrayList<>();
        
        if (!Files.exists(Paths.get(fileName))) {
            return entries;
        }
        
        try (FileChannel readChannel = FileChannel.open(Paths.get(fileName), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            
            while (readChannel.read(buffer) > 0) {
                buffer.flip();
                
                while (buffer.remaining() > 0) {
                    if (buffer.remaining() < 13) break;
                    
                    int operation = buffer.get();
                    long timestamp = buffer.getLong();
                    int keyLength = buffer.getInt();
                    
                    if (buffer.remaining() < keyLength + 4) break;
                    
                    byte[] keyBytes = new byte[keyLength];
                    buffer.get(keyBytes);
                    String key = new String(keyBytes);
                    
                    int valueLength = buffer.getInt();
                    if (buffer.remaining() < valueLength) break;
                    
                    byte[] value = null;
                    if (valueLength > 0) {
                        value = new byte[valueLength];
                        buffer.get(value);
                    }
                    
                    WALEntry entry = new WALEntry(
                        WALOperation.values()[operation], key, value);
                    entries.add(entry);
                }
                
                buffer.compact();
            }
        }
        
        return entries;
    }
    
    void truncate() throws IOException {
        channel.truncate(0);
    }
    
    void close() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }
}

class DataPointer {
    final String fileName;
    final long offset;
    final int length;
    
    DataPointer(String key, int length) {
        this.fileName = "memtable";
        this.offset = 0;
        this.length = length;
    }
    
    DataPointer(String fileName, long offset, int length) {
        this.fileName = fileName;
        this.offset = offset;
        this.length = length;
    }
}

class BufferPool {
    private final Map<Long, Page> pages;
    private final LinkedList<Long> lruOrder;
    private final int maxPages;
    private final int pageSize;
    
    BufferPool(int maxPages, int pageSize) {
        this.pages = new ConcurrentHashMap<>();
        this.lruOrder = new LinkedList<>();
        this.maxPages = maxPages;
        this.pageSize = pageSize;
    }
    
    Page getPage(long pageId) {
        Page page = pages.get(pageId);
        if (page != null) {
            updateLRU(pageId);
            return page;
        }
        
        if (pages.size() >= maxPages) {
            evictLRU();
        }
        
        page = loadPage(pageId);
        pages.put(pageId, page);
        lruOrder.addFirst(pageId);
        
        return page;
    }
    
    private void updateLRU(long pageId) {
        lruOrder.remove(pageId);
        lruOrder.addFirst(pageId);
    }
    
    private void evictLRU() {
        if (!lruOrder.isEmpty()) {
            long pageId = lruOrder.removeLast();
            Page page = pages.remove(pageId);
            if (page != null && page.isDirty()) {
                flushPage(page);
            }
        }
    }
    
    private Page loadPage(long pageId) {
        return new Page(pageId, new byte[pageSize]);
    }
    
    private void flushPage(Page page) {
        // Implementation for writing page to disk
    }
}

class Page {
    private final long pageId;
    private final byte[] data;
    private boolean dirty;
    
    Page(long pageId, byte[] data) {
        this.pageId = pageId;
        this.data = data;
        this.dirty = false;
    }
    
    public long getPageId() { return pageId; }
    public byte[] getData() { return data; }
    public boolean isDirty() { return dirty; }
    public void markDirty() { dirty = true; }
}
