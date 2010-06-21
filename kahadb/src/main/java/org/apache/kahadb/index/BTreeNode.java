/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kahadb.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.kahadb.index.BTreeIndex.Prefixer;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.VariableMarshaller;


/**
 * The BTreeNode class represents a node in the BTree object graph.  It is stored in 
 * one Page of a PageFile.
 */
public final class BTreeNode<Key,Value> {

    // The index that this node is part of.
    private final BTreeIndex<Key,Value> index;
    // The parent node or null if this is the root node of the BTree
    private BTreeNode<Key,Value> parent;
    // The page associated with this node
    private Page<BTreeNode<Key,Value>> page;
    
    // Order list of keys in the node
    private Key[] keys;
    // Values associated with the Keys. Null if this is a branch node.
    private Value[] values;
    // nodeId pointers to children BTreeNodes. Null if this is a leaf node.
    private long[] children;
    // The next leaf node after this one.  Used for fast iteration of the entries.
    private long next = -1;
    
    private final class KeyValueEntry implements Map.Entry<Key, Value> {
        private final Key key;
        private final Value value;

        public KeyValueEntry(Key key, Value value) {
            this.key = key;
            this.value = value;
        }

        public Key getKey() {
            return key;
        }

        public Value getValue() {
            return value;
        }

        public Value setValue(Value value) {
            throw new UnsupportedOperationException();
        }

    }

    private final class BTreeIterator implements Iterator<Map.Entry<Key, Value>> {
        
        private final Transaction tx;
        BTreeNode<Key,Value> current;
        int nextIndex;
        Map.Entry<Key,Value> nextEntry;

        private BTreeIterator(Transaction tx, BTreeNode<Key,Value> current, int nextIndex) {
            this.tx = tx;
            this.current = current;
            this.nextIndex=nextIndex;
        }

        synchronized private void findNextPage() {
            if( nextEntry!=null ) {
                return;
            }
            
            try {
                while( current!=null ) {
                    if( nextIndex >= current.keys.length ) {
                        // we need to roll to the next leaf..
                        if( current.next >= 0 ) {
                            current = index.loadNode(tx, current.next, null);
                            assert !current.isBranch() : "Should have linked to the next leaf node.";
                            nextIndex=0;
                        } else {
                            break;
                        }
                    }  else {
                        nextEntry = new KeyValueEntry(current.keys[nextIndex], current.values[nextIndex]);
                        nextIndex++;
                        break;
                    }
                    
                }
            } catch (IOException e) {
            }
        }

        public boolean hasNext() {
            findNextPage();
            return nextEntry !=null;
        }

        public Entry<Key, Value> next() {
            findNextPage(); 
            if( nextEntry !=null ) {
                Entry<Key, Value> lastEntry = nextEntry;
                nextEntry=null;
                return lastEntry;
            } else {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The Marshaller is used to store and load the data in the BTreeNode into a Page.
     *  
     * @param <Key>
     * @param <Value>
     */
    static public class Marshaller<Key,Value> extends VariableMarshaller<BTreeNode<Key,Value>> {
        private final BTreeIndex<Key,Value> index;
        
        public Marshaller(BTreeIndex<Key,Value> index) {
            this.index = index;
        }

        public void writePayload(BTreeNode<Key,Value> node, DataOutput os) throws IOException {
            // Write the keys
            short count = (short)node.keys.length; // cast may truncate value...
            if( count != node.keys.length ) {
                throw new IOException("Too many keys");
            }
            
            os.writeShort(count);
            for (int i = 0; i < node.keys.length; i++) {
                index.getKeyMarshaller().writePayload(node.keys[i], os);
            }
            
            if( node.isBranch() ) {
                // If this is a branch...
                os.writeBoolean(true);
                for (int i = 0; i < count+1; i++) {
                    os.writeLong(node.children[i]);
                }
                
            } else {
                // If this is a leaf
                os.writeBoolean(false);
                for (int i = 0; i < count; i++) {
                    index.getValueMarshaller().writePayload(node.values[i], os);
                }
                os.writeLong(node.next);
            }
        }

        @SuppressWarnings("unchecked")
        public BTreeNode<Key,Value> readPayload(DataInput is) throws IOException {
            BTreeNode<Key,Value>  node = new BTreeNode<Key,Value>(index);
            int count = is.readShort();
            
            node.keys = (Key[])new Object[count];
            for (int i = 0; i < count; i++) {
                node.keys[i] = index.getKeyMarshaller().readPayload(is);
            }
            
            if( is.readBoolean() ) {
                node.children = new long[count+1];
                for (int i = 0; i < count+1; i++) {
                    node.children[i] = is.readLong();
                }
            } else {
                node.values = (Value[])new Object[count];
                for (int i = 0; i < count; i++) {
                    node.values[i] = index.getValueMarshaller().readPayload(is);
                }
                node.next = is.readLong();
            }
            return node;
        }
    }

    public BTreeNode(BTreeIndex<Key,Value> index) {
        this.index = index;
    }
    
    public void setEmpty() {
        setLeafData(createKeyArray(0), createValueArray(0));
    }
    

    /**
     * Internal (to the BTreeNode) method. Because this method is called only by
     * BTreeNode itself, no synchronization done inside of this method.
     * @throws IOException 
     */
    private BTreeNode<Key,Value> getChild(Transaction tx, int idx) throws IOException {
        if (isBranch() && idx >= 0 && idx < children.length) {
            BTreeNode<Key, Value> result = this.index.loadNode(tx, children[idx], this);
            return result;
        } else {
            return null;
        }
    }


    /**
     * Returns the right most leaf from the current btree graph.
     * @throws IOException
     */
    private BTreeNode<Key,Value> getRightLeaf(Transaction tx) throws IOException {
        BTreeNode<Key,Value> cur = this;
        while(cur.isBranch()) {
            cur = cur.getChild(tx, keys.length);
        }
        return cur;
    }

    /**
     * Returns the left most leaf from the current btree graph.
     * @throws IOException
     */
    private BTreeNode<Key,Value> getLeftLeaf(Transaction tx) throws IOException {
        BTreeNode<Key,Value> cur = this;
        while(cur.isBranch()) {
            cur = cur.getChild(tx, 0);
        }
        return cur;
    }

    /**
     * Returns the left most leaf from the current btree graph.
     * @throws IOException
     */
    private BTreeNode<Key,Value> getLeftPeer(Transaction tx, BTreeNode<Key,Value> x) throws IOException {
        BTreeNode<Key,Value> cur = x;
        while( cur.parent !=null ) {
            if( cur.parent.children[0] == cur.getPageId() ) {
                cur = cur.parent;
            } else {
                for( int i=0; i < cur.parent.children.length; i ++) {
                    if( cur.parent.children[i]==cur.getPageId() ) {
                        return  cur.parent.getChild(tx, i-1);
                    }
                }
                throw new AssertionError("page "+x+" was decendent of "+cur.getPageId());
            }
        }
        return null;
    }

    public Value remove(Transaction tx, Key key) throws IOException {

        if(isBranch()) {
            int idx = Arrays.binarySearch(keys, key);
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            BTreeNode<Key, Value> child = getChild(tx, idx);
            if( child.getPageId() == index.getPageId() ) {
                throw new IOException("BTree corrupted: Cylce detected.");
            }
            Value rc = child.remove(tx, key);
            
            // child node is now empty.. remove it from the branch node.
            if( child.keys.length == 0 ) {
                
                // If the child node is a branch, promote
                if( child.isBranch() ) {
                    // This is cause branches are never really empty.. they just go down to 1 child..
                    children[idx] = child.children[0];
                } else {
                    
                    // The child was a leaf. Then we need to actually remove it from this branch node..
                    // and relink the previous leaf to skip to the next leaf.

                    BTreeNode<Key, Value> previousLeaf = null;
                    if( idx > 0 ) {
                        // easy if we this node hold the previous child.
                        previousLeaf = getChild(tx, idx-1).getRightLeaf(tx);
                    } else {
                        // less easy if we need to go to the parent to find the previous child.
                        BTreeNode<Key, Value> lp = getLeftPeer(tx, this);
                        if( lp!=null ) {
                            previousLeaf = lp.getRightLeaf(tx);
                        }
                        // lp will be null if there was no previous child.
                    }

                    if( previousLeaf !=null ) {
                        previousLeaf.next = child.next;
                        index.storeNode(tx, previousLeaf, true);
                    }

                    if( idx < children.length-1 ) {
                        // Delete it and key to the right.
                        setBranchData(arrayDelete(keys, idx), arrayDelete(children, idx));
                    } else {
                        // It was the last child.. Then delete it and key to the left
                        setBranchData(arrayDelete(keys, idx-1), arrayDelete(children, idx));
                    }
                    
                    // If we are the root node, and only have 1 child left.  Then 
                    // make the root be the leaf node.
                    if( children.length == 1 && parent==null ) {
                        child = getChild(tx, 0);
                        keys = child.keys;
                        children = child.children;
                        values = child.values;
                        // free up the page..
                        tx.free(child.getPage());
                    }
                    
                }
                index.storeNode(tx, this, true);
            }
            
            return rc;
        } else {
            int idx = Arrays.binarySearch(keys, key);
            if (idx < 0) {
                return null;
            } else {
                Value oldValue = values[idx];
                setLeafData(arrayDelete(keys, idx), arrayDelete(values, idx));
                
                if( keys.length==0 && parent!=null) {
                    tx.free(getPage());
                } else {
                    index.storeNode(tx, this, true);
                }
                
                return oldValue;
            }
        }
    }

    public Value put(Transaction tx, Key key, Value value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if( isBranch() ) {
            return getLeafNode(tx, this, key).put(tx, key, value);
        } else {
            int idx = Arrays.binarySearch(keys, key);
            
            Value oldValue=null;
            if (idx >= 0) {
                // Key was found... Overwrite
                oldValue = values[idx];
                values[idx] = value;
                setLeafData(keys, values);
            } else {
                // Key was not found, Insert it
                idx = -(idx + 1);
                setLeafData(arrayInsert(keys, key, idx), arrayInsert(values, value, idx));
            }
            
            try {
                index.storeNode(tx, this, allowOverflow());
            } catch ( Transaction.PageOverflowIOException e ) {
                // If we get an overflow 
                split(tx);
            }
            
            return oldValue;
        }
    }

    private void promoteValue(Transaction tx, Key key, long nodeId) throws IOException {

        int idx = Arrays.binarySearch(keys, key);
        idx = idx < 0 ? -(idx + 1) : idx + 1;
        setBranchData(arrayInsert(keys, key, idx), arrayInsert(children, nodeId, idx + 1));

        try {
            index.storeNode(tx, this, allowOverflow());
        } catch ( Transaction.PageOverflowIOException e ) {
            split(tx);
        }

    }

    /**
     * Internal to the BTreeNode method
     */
    private void split(Transaction tx) throws IOException {
        Key[] leftKeys;
        Key[] rightKeys;
        Value[] leftValues=null;
        Value[] rightValues=null;
        long[] leftChildren=null;
        long[] rightChildren=null;
        Key separator;

        int vc = keys.length;
        int pivot = vc / 2;

        // Split the node into two nodes
        if( isBranch() ) {

            leftKeys = createKeyArray(pivot);
            leftChildren = new long[leftKeys.length + 1];
            rightKeys = createKeyArray(vc - (pivot + 1));
            rightChildren = new long[rightKeys.length + 1];

            System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
            System.arraycopy(children, 0, leftChildren, 0, leftChildren.length);
            System.arraycopy(keys, leftKeys.length + 1, rightKeys, 0, rightKeys.length);
            System.arraycopy(children, leftChildren.length, rightChildren, 0, rightChildren.length);

            // Is it a Simple Prefix BTree??
            Prefixer<Key> prefixer = index.getPrefixer();
            if(prefixer!=null) {
                separator = prefixer.getSimplePrefix(leftKeys[leftKeys.length - 1], rightKeys[0]);
            } else {
                separator = keys[leftKeys.length];
            }
                
            
        } else {

            leftKeys = createKeyArray(pivot);
            leftValues = createValueArray(leftKeys.length);
            rightKeys = createKeyArray(vc - pivot);
            rightValues = createValueArray(rightKeys.length);

            System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
            System.arraycopy(values, 0, leftValues, 0, leftValues.length);
            System.arraycopy(keys, leftKeys.length, rightKeys, 0, rightKeys.length);
            System.arraycopy(values, leftValues.length, rightValues, 0, rightValues.length);

            // separator = getSeparator(leftVals[leftVals.length - 1],
            // rightVals[0]);
            separator = rightKeys[0];

        }

        // Promote the pivot to the parent branch
        if (parent == null) {
            
            // This can only happen if this is the root
            BTreeNode<Key,Value> rNode = this.index.createNode(tx, this);
            BTreeNode<Key,Value> lNode = this.index.createNode(tx, this);

            if( isBranch() ) {
                rNode.setBranchData(rightKeys, rightChildren);
                lNode.setBranchData(leftKeys, leftChildren);
            } else {
                rNode.setLeafData(rightKeys, rightValues);
                lNode.setLeafData(leftKeys, leftValues);
                lNode.setNext(rNode.getPageId());
            }

            Key[] v = createKeyArray(1);
            v[0]=separator;
            setBranchData(v, new long[] { lNode.getPageId(), rNode.getPageId() });

            index.storeNode(tx, this, true);
            index.storeNode(tx, rNode, true);
            index.storeNode(tx, lNode, true);
            
        } else {
            BTreeNode<Key,Value> rNode = this.index.createNode(tx, parent);
            
            if( isBranch() ) {
                setBranchData(leftKeys, leftChildren);
                rNode.setBranchData(rightKeys, rightChildren);
            } else {
                rNode.setNext(next);
                next = rNode.getPageId();
                setLeafData(leftKeys, leftValues);
                rNode.setLeafData(rightKeys, rightValues);
            }

            index.storeNode(tx, this, true);
            index.storeNode(tx, rNode, true);
            parent.promoteValue(tx, separator, rNode.getPageId());
        }
    }

    public void printStructure(Transaction tx, PrintWriter out, String prefix) throws IOException {
        if( prefix.length()>0 && parent == null ) {
            throw new IllegalStateException("Cycle back to root node detected.");
        }
        
        if( isBranch() ) {
            for(int i=0 ; i < children.length; i++) {
                BTreeNode<Key, Value> child = getChild(tx, i);
                if( i == children.length-1) {
                    out.println(prefix+"\\- "+child.getPageId()+(child.isBranch()?" ("+child.children.length+")":""));
                    child.printStructure(tx, out, prefix+"   ");
                } else {
                    out.println(prefix+"|- "+child.getPageId()+(child.isBranch()?" ("+child.children.length+")":"")+" : "+keys[i]);
                    child.printStructure(tx, out, prefix+"   ");
                }
            }
        }
    }
    
    
    public int getMinLeafDepth(Transaction tx, int depth) throws IOException {
        depth++;
        if( isBranch() ) {
            int min = Integer.MAX_VALUE;
            for(int i=0 ; i < children.length; i++) {
                min = Math.min(min, getChild(tx, i).getMinLeafDepth(tx, depth));
            }
            return min;
        } else {
//            print(depth*2, "- "+page.getPageId());
            return depth;
        }
    }

    public int getMaxLeafDepth(Transaction tx, int depth) throws IOException {
        depth++;
        if( isBranch() ) {
            int v = 0;
            for(int i=0 ; i < children.length; i++) {
                v = Math.max(v, getChild(tx, i).getMaxLeafDepth(tx, depth));
            }
            depth = v;
        } 
        return depth;
    }

    public Value get(Transaction tx, Key key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if( isBranch() ) {
            return getLeafNode(tx, this, key).get(tx, key);
        } else {
            int idx = Arrays.binarySearch(keys, key);
            if (idx < 0) {
                return null;
            } else {
                return values[idx];
            }
        }
    }
    
    public boolean isEmpty(final Transaction tx) throws IOException {
        return keys.length==0;
    }

    public void visit(Transaction tx, BTreeVisitor<Key, Value> visitor) throws IOException {
        if (visitor == null) {
            throw new IllegalArgumentException("Visitor cannot be null");
        }
        if( isBranch() ) {
            for(int i=0; i < this.children.length; i++) {
                Key key1 = null;
                if( i!=0 ) {
                    key1 = keys[i-1];
                }
                Key key2 = null;
                if( i!=this.children.length-1 ) {
                    key2 = keys[i];
                }
                if( visitor.isInterestedInKeysBetween(key1, key2) ) {
                    BTreeNode<Key, Value> child = getChild(tx, i);
                    child.visit(tx, visitor);
                }
            }
        } else {
            visitor.visit(Arrays.asList(keys), Arrays.asList(values));
        }
    }
    
    public Map.Entry<Key,Value> getFirst(Transaction tx) throws IOException {
        BTreeNode<Key, Value> node = this;
        while( node .isBranch() ) {
            node = node.getChild(tx, 0);
        }
        if( node.values.length>0 ) {
            return new KeyValueEntry(node.keys[0], node.values[0]);
        } else {
            return null;
        }
    }

    public Map.Entry<Key,Value> getLast(Transaction tx) throws IOException {
        BTreeNode<Key, Value> node = this;
        while( node.isBranch() ) {
            node = node.getChild(tx, node.children.length-1);
        }
        if( node.values.length>0 ) {
            int idx = node.values.length-1;
            return new KeyValueEntry(node.keys[idx], node.values[idx]);
        } else {
            return null;
        }
    }
    
    public BTreeNode<Key,Value> getFirstLeafNode(Transaction tx) throws IOException {
        BTreeNode<Key, Value> node = this;
        while( node .isBranch() ) {
            node = node.getChild(tx, 0);
        }
        return node;
    }
    
    public Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx, Key startKey) throws IOException {
        if (startKey == null) {
            return iterator(tx);
        }
        if( isBranch() ) {
            return getLeafNode(tx, this, startKey).iterator(tx, startKey);
        } else {
            int idx = Arrays.binarySearch(keys, startKey);
            if (idx < 0) {
                idx = -(idx + 1);
            }
            return new BTreeIterator(tx, this, idx);
        }
    }

    public Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx) throws IOException {
        return new BTreeIterator(tx, getFirstLeafNode(tx), 0);
    }
    
    public void clear(Transaction tx) throws IOException {
        if( isBranch() ) {
            for (int i = 0; i < children.length; i++) {
                BTreeNode<Key, Value> node = index.loadNode(tx, children[i], this);
                node.clear(tx);
                tx.free(node.getPage());
            }
        }
        // Reset the root node to be a leaf.
        if( parent == null ) {
            setLeafData(createKeyArray(0), createValueArray(0));
            next=-1;
            index.storeNode(tx, this, true);
        }
    }


    private static <Key,Value> BTreeNode<Key, Value> getLeafNode(Transaction tx, final BTreeNode<Key, Value> node, Key key) throws IOException {
        BTreeNode<Key, Value> current = node;
        while( true ) {
            if( current.isBranch() ) {
                int idx = Arrays.binarySearch(current.keys, key);
                idx = idx < 0 ? -(idx + 1) : idx + 1;
                BTreeNode<Key, Value> child = current.getChild(tx, idx);        

                // A little cycle detection for sanity's sake
                if( child == node ) {
                    throw new IOException("BTree corrupted: Cylce detected.");
                }
                
                current = child;
            } else {
                break;
            }
        }
        return current;
    }

    public boolean contains(Transaction tx, Key key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if( isBranch() ) {
            return getLeafNode(tx, this, key).contains(tx, key);
        } else {
            int idx = Arrays.binarySearch(keys, key);
            if (idx < 0) {
                return false;
            } else {
                return true;
            }
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Implementation methods
    ///////////////////////////////////////////////////////////////////
 

    private boolean allowOverflow() {
        // Only allow page overflow if there are <= 3 keys in the node.  Otherwise a split will occur on overflow
        return this.keys.length<=3;
    }


    private void setLeafData(Key[] keys, Value[] values) {
        this.keys = keys;
        this.values = values;
        this.children = null;
    }
    
    private void setBranchData(Key[] keys, long[] nodeIds) {
        this.keys = keys;
        this.children = nodeIds;
        this.values = null;
    }

    @SuppressWarnings("unchecked")
    private Key[] createKeyArray(int size) {
        return (Key[])new Object[size];
    }

    @SuppressWarnings("unchecked")
    private Value[] createValueArray(int size) {
        return (Value[])new Object[size];
    }
    
    @SuppressWarnings("unchecked")
    static private <T> T[] arrayDelete(T[] vals, int idx) {
        T[] newVals = (T[])new Object[vals.length - 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        if (idx < newVals.length) {
            System.arraycopy(vals, idx + 1, newVals, idx, newVals.length - idx);
        }
        return newVals;
    }
    
    static private long[] arrayDelete(long[] vals, int idx) {
        long[] newVals = new long[vals.length - 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        if (idx < newVals.length) {
            System.arraycopy(vals, idx + 1, newVals, idx, newVals.length - idx);
        }
        return newVals;
    }

    @SuppressWarnings("unchecked")
    static private <T> T[] arrayInsert(T[] vals, T val, int idx) {
        T[] newVals = (T[])new Object[vals.length + 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        newVals[idx] = val;
        if (idx < vals.length) {
            System.arraycopy(vals, idx, newVals, idx + 1, vals.length - idx);
        }
        return newVals;
    }


    static private long[] arrayInsert(long[] vals, long val, int idx) {
        
        long[] newVals = new long[vals.length + 1];
        if (idx > 0) {
            System.arraycopy(vals, 0, newVals, 0, idx);
        }
        newVals[idx] = val;
        if (idx < vals.length) {
            System.arraycopy(vals, idx, newVals, idx + 1, vals.length - idx);
        }
        return newVals;
    }

    ///////////////////////////////////////////////////////////////////
    // Property Accessors
    ///////////////////////////////////////////////////////////////////
    private boolean isBranch() {
        return children!=null;
    }

    public long getPageId() {
        return page.getPageId();
    }

    public BTreeNode<Key, Value> getParent() {
        return parent;
    }

    public void setParent(BTreeNode<Key, Value> parent) {
        this.parent = parent;
    }

    public Page<BTreeNode<Key, Value>> getPage() {
        return page;
    }

    public void setPage(Page<BTreeNode<Key, Value>> page) {
        this.page = page;
    }

    public long getNext() {
        return next;
    }

    public void setNext(long next) {
        this.next = next;
    }
    
    @Override
    public String toString() {
        return "[BTreeNode "+(isBranch()?"branch":"leaf")+": "+Arrays.asList(keys)+"]";
    }

}


