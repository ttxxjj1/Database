package db.storage;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BPlusTree {
    private Node root;
    private final int order;
    private final ReadWriteLock lock;
    
    public BPlusTree(int order) {
        this.order = order;
        this.lock = new ReentrantReadWriteLock();
        this.root = new LeafNode(order);
    }
    
    public void put(String key, DataPointer value) {
        lock.writeLock().lock();
        try {
            root = root.insert(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public DataPointer get(String key) {
        lock.readLock().lock();
        try {
            return root.search(key);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void delete(String key) {
        lock.writeLock().lock();
        try {
            root = root.delete(key);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public List<String> range(String startKey, String endKey, int limit) {
        lock.readLock().lock();
        try {
            List<String> result = new ArrayList<>();
            LeafNode startLeaf = findLeaf(startKey);
            
            if (startLeaf == null) return result;
            
            LeafNode current = startLeaf;
            while (current != null && result.size() < limit) {
                for (int i = 0; i < current.keys.size(); i++) {
                    String key = current.keys.get(i);
                    if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                        result.add(key);
                        if (result.size() >= limit) break;
                    }
                }
                current = current.next;
            }
            
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    private LeafNode findLeaf(String key) {
        Node current = root;
        while (current instanceof InternalNode) {
            current = ((InternalNode) current).findChild(key);
        }
        return (LeafNode) current;
    }
    
    abstract class Node {
        final List<String> keys;
        final int order;
        
        Node(int order) {
            this.order = order;
            this.keys = new ArrayList<>();
        }
        
        abstract Node insert(String key, DataPointer value);
        abstract DataPointer search(String key);
        abstract Node delete(String key);
        abstract boolean isLeaf();
        abstract boolean isFull();
        abstract boolean isUnderflow();
    }
    
    class InternalNode extends Node {
        final List<Node> children;
        
        InternalNode(int order) {
            super(order);
            this.children = new ArrayList<>();
        }
        
        @Override
        Node insert(String key, DataPointer value) {
            Node child = findChild(key);
            Node newChild = child.insert(key, value);
            
            if (newChild != child) {
                int index = children.indexOf(child);
                children.set(index, newChild);
                
                if (newChild instanceof SplitResult) {
                    SplitResult split = (SplitResult) newChild;
                    keys.add(index, split.separatorKey);
                    children.add(index + 1, split.rightNode);
                    
                    if (isFull()) {
                        return split();
                    }
                }
            }
            
            return this;
        }
        
        @Override
        DataPointer search(String key) {
            return findChild(key).search(key);
        }
        
        @Override
        Node delete(String key) {
            Node child = findChild(key);
            Node result = child.delete(key);
            
            int index = children.indexOf(child);
            children.set(index, result);
            
            if (result != null && result.isUnderflow()) {
                rebalance(index);
            }
            
            if (keys.isEmpty() && !children.isEmpty()) {
                return children.get(0);
            }
            
            return this;
        }
        
        private void rebalance(int index) {
            Node child = children.get(index);
            
            if (index > 0) {
                Node leftSibling = children.get(index - 1);
                if (leftSibling.keys.size() > leftSibling.order / 2) {
                    borrowFromLeft(index);
                    return;
                }
            }
            
            if (index < children.size() - 1) {
                Node rightSibling = children.get(index + 1);
                if (rightSibling.keys.size() > rightSibling.order / 2) {
                    borrowFromRight(index);
                    return;
                }
            }
            
            if (index > 0) {
                mergeWithLeft(index);
            } else if (index < children.size() - 1) {
                mergeWithRight(index);
            }
        }
        
        private void borrowFromLeft(int index) {
            Node child = children.get(index);
            Node leftSibling = children.get(index - 1);
            
            String separatorKey = keys.get(index - 1);
            child.keys.add(0, separatorKey);
            
            String newSeparator = leftSibling.keys.remove(leftSibling.keys.size() - 1);
            keys.set(index - 1, newSeparator);
            
            if (child instanceof InternalNode) {
                InternalNode internalChild = (InternalNode) child;
                InternalNode internalLeft = (InternalNode) leftSibling;
                internalChild.children.add(0, 
                    internalLeft.children.remove(internalLeft.children.size() - 1));
            }
        }
        
        private void borrowFromRight(int index) {
            Node child = children.get(index);
            Node rightSibling = children.get(index + 1);
            
            String separatorKey = keys.get(index);
            child.keys.add(separatorKey);
            
            String newSeparator = rightSibling.keys.remove(0);
            keys.set(index, newSeparator);
            
            if (child instanceof InternalNode) {
                InternalNode internalChild = (InternalNode) child;
                InternalNode internalRight = (InternalNode) rightSibling;
                internalChild.children.add(internalRight.children.remove(0));
            }
        }
        
        private void mergeWithLeft(int index) {
            Node child = children.get(index);
            Node leftSibling = children.get(index - 1);
            
            String separatorKey = keys.remove(index - 1);
            leftSibling.keys.add(separatorKey);
            leftSibling.keys.addAll(child.keys);
            
            if (leftSibling instanceof InternalNode) {
                InternalNode internalLeft = (InternalNode) leftSibling;
                InternalNode internalChild = (InternalNode) child;
                internalLeft.children.addAll(internalChild.children);
            }
            
            children.remove(index);
        }
        
        private void mergeWithRight(int index) {
            Node child = children.get(index);
            Node rightSibling = children.get(index + 1);
            
            String separatorKey = keys.remove(index);
            child.keys.add(separatorKey);
            child.keys.addAll(rightSibling.keys);
            
            if (child instanceof InternalNode) {
                InternalNode internalChild = (InternalNode) child;
                InternalNode internalRight = (InternalNode) rightSibling;
                internalChild.children.addAll(internalRight.children);
            }
            
            children.remove(index + 1);
        }
        
        Node findChild(String key) {
            int index = Collections.binarySearch(keys, key);
            if (index >= 0) {
                return children.get(index + 1);
            } else {
                index = -index - 1;
                return children.get(index);
            }
        }
        
        private SplitResult split() {
            int mid = keys.size() / 2;
            String separatorKey = keys.get(mid);
            
            InternalNode right = new InternalNode(order);
            right.keys.addAll(keys.subList(mid + 1, keys.size()));
            right.children.addAll(children.subList(mid + 1, children.size()));
            
            keys.subList(mid, keys.size()).clear();
            children.subList(mid + 1, children.size()).clear();
            
            return new SplitResult(this, right, separatorKey);
        }
        
        @Override
        boolean isLeaf() { return false; }
        
        @Override
        boolean isFull() { return keys.size() >= order - 1; }
        
        @Override
        boolean isUnderflow() { return keys.size() < order / 2; }
    }
    
    class LeafNode extends Node {
        final List<DataPointer> values;
        LeafNode next;
        LeafNode prev;
        
        LeafNode(int order) {
            super(order);
            this.values = new ArrayList<>();
        }
        
        @Override
        Node insert(String key, DataPointer value) {
            int index = Collections.binarySearch(keys, key);
            if (index >= 0) {
                values.set(index, value);
                return this;
            }
            
            index = -index - 1;
            keys.add(index, key);
            values.add(index, value);
            
            if (isFull()) {
                return split();
            }
            
            return this;
        }
        
        @Override
        DataPointer search(String key) {
            int index = Collections.binarySearch(keys, key);
            return index >= 0 ? values.get(index) : null;
        }
        
        @Override
        Node delete(String key) {
            int index = Collections.binarySearch(keys, key);
            if (index >= 0) {
                keys.remove(index);
                values.remove(index);
            }
            
            return isUnderflow() ? null : this;
        }
        
        private SplitResult split() {
            int mid = keys.size() / 2;
            
            LeafNode right = new LeafNode(order);
            right.keys.addAll(keys.subList(mid, keys.size()));
            right.values.addAll(values.subList(mid, values.size()));
            
            keys.subList(mid, keys.size()).clear();
            values.subList(mid, values.size()).clear();
            
            right.next = this.next;
            right.prev = this;
            if (this.next != null) {
                this.next.prev = right;
            }
            this.next = right;
            
            return new SplitResult(this, right, right.keys.get(0));
        }
        
        @Override
        boolean isLeaf() { return true; }
        
        @Override
        boolean isFull() { return keys.size() >= order; }
        
        @Override
        boolean isUnderflow() { return keys.size() < order / 2; }
    }
    
    class SplitResult extends Node {
        final Node leftNode;
        final Node rightNode;
        final String separatorKey;
        
        SplitResult(Node leftNode, Node rightNode, String separatorKey) {
            super(0);
            this.leftNode = leftNode;
            this.rightNode = rightNode;
            this.separatorKey = separatorKey;
        }
        
        @Override
        Node insert(String key, DataPointer value) { return null; }
        
        @Override
        DataPointer search(String key) { return null; }
        
        @Override
        Node delete(String key) { return null; }
        
        @Override
        boolean isLeaf() { return false; }
        
        @Override
        boolean isFull() { return false; }
        
        @Override
        boolean isUnderflow() { return false; }
    }
