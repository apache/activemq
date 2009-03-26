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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.Marshaller;

/**
 * BTreeIndex represents a Variable Magnitude B+Tree in a Page File.
 * A BTree is a bit flexible in that it can be used for set or
 * map-based indexing.  Leaf nodes are linked together for faster
 * iteration of the values. 
 *
 * <br>
 * The Variable Magnitude attribute means that the BTree attempts
 * to store as many values and pointers on one page as is possible.
 * 
 * <br>
 * The implementation can optionally a be Simple-Prefix B+Tree.
 * 
 * <br>
 * For those who don't know how a Simple-Prefix B+Tree works, the primary
 * distinction is that instead of promoting actual keys to branch pages,
 * when leaves are split, a shortest-possible separator is generated at
 * the pivot.  That separator is what is promoted to the parent branch
 * (and continuing up the list).  As a result, actual keys and pointers
 * can only be found at the leaf level.  This also affords the index the
 * ability to ignore costly merging and redistribution of pages when
 * deletions occur.  Deletions only affect leaf pages in this
 * implementation, and so it is entirely possible for a leaf page to be
 * completely empty after all of its keys have been removed.
 *
 * @version $Revision$, $Date$
 */
public class BTreeIndex<Key,Value> implements Index<Key,Value> {

    private static final Log LOG = LogFactory.getLog(BTreeIndex.class);

    /**
     * Interface used to determine the simple prefix of two keys.
     *
     * @version $Revision$, $Date$
     */
    static public interface Prefixer<Key> {
        
        /**
         * This methods should return shortest prefix of value2 where the following still holds:<br/>
         * value1 <= prefix <= value2.<br/><br/>
         * 
         * When this method is called, the following is guaranteed:<br/>
         * value1 < value2<br/><br/>
         * 
         * 
         * @param value1
         * @param value2
         * @return
         */
        public Key getSimplePrefix(Key value1, Key value2);
    }
    
    /**
     * StringPrefixer is a Prefixer implementation that works on strings.
     */
    static public class StringPrefixer implements Prefixer<String> {
        
        /**
         * Example:
         * If value1 is "Hello World"
         * and value 2 is "Help Me"
         * then the result will be: "Help"
         * 
         * @see  Prefixer#getSimplePrefix
         */
        public String getSimplePrefix(String value1, String value2) {
            char[] c1 = value1.toCharArray();
            char[] c2 = value2.toCharArray();
            int n = Math.min(c1.length, c2.length);
            int i =0;
            while (i < n) {
                if (c1[i] != c2[i]) {
                    return value2.substring(0,i+1);
                }
                i++;
            }
            
            if( n == c2.length ) {
                return value2;
            }
            return value2.substring(0,n);
        }
    }    

    private PageFile pageFile;
    private long pageId;
    private AtomicBoolean loaded = new AtomicBoolean();
    
    private final BTreeNode.Marshaller<Key, Value> marshaller = new BTreeNode.Marshaller<Key, Value>(this);
    private Marshaller<Key> keyMarshaller;
    private Marshaller<Value> valueMarshaller;
    private Prefixer<Key> prefixer;

    public BTreeIndex() {
    }

    public BTreeIndex(long rootPageId) {
        this.pageId = rootPageId;
    }
    
    @SuppressWarnings("unchecked")
    public BTreeIndex(Page page) {
        this(page.getPageId());
    }
    
    public BTreeIndex(PageFile pageFile, long rootPageId) {
        this.pageFile = pageFile;
        this.pageId = rootPageId;
    }

    @SuppressWarnings("unchecked")
    public BTreeIndex(PageFile pageFile, Page page) {
        this(pageFile, page.getPageId());
    }

    synchronized public void load(Transaction tx) throws IOException {
        if (loaded.compareAndSet(false, true)) {
            LOG.debug("loading");
            if( keyMarshaller == null ) {
                throw new IllegalArgumentException("The key marshaller must be set before loading the BTreeIndex");
            }
            if( valueMarshaller == null ) {
                throw new IllegalArgumentException("The value marshaller must be set before loading the BTreeIndex");
            }
            
            final Page<BTreeNode<Key,Value>> p = tx.load(pageId, null);
            if( p.getType() == Page.PAGE_FREE_TYPE ) {
                 // Need to initialize it..
                BTreeNode<Key, Value> root = createNode(p, null);
                storeNode(tx, root, true);
            }
        }
    }
    
    synchronized public void unload(Transaction tx) {
        if (loaded.compareAndSet(true, false)) {
        }    
    }
    
    private BTreeNode<Key,Value> getRoot(Transaction tx) throws IOException {
        return loadNode(tx, pageId, null);
    }
    
    synchronized public boolean containsKey(Transaction tx, Key key) throws IOException {
        assertLoaded();
        return getRoot(tx).contains(tx, key);
    }

    synchronized public Value get(Transaction tx, Key key) throws IOException {
        assertLoaded();
        return getRoot(tx).get(tx, key);
    }

    synchronized public Value put(Transaction tx, Key key, Value value) throws IOException {
        assertLoaded();
        return getRoot(tx).put(tx, key, value);
    }

    synchronized public Value remove(Transaction tx, Key key) throws IOException {
        assertLoaded();
        return getRoot(tx).remove(tx, key);
    }
    
    public boolean isTransient() {
        return false;
    }

    synchronized public void clear(Transaction tx) throws IOException {
        getRoot(tx).clear(tx);
    }

    synchronized public int getMinLeafDepth(Transaction tx) throws IOException {
        return getRoot(tx).getMinLeafDepth(tx, 0);
    }

    synchronized public int getMaxLeafDepth(Transaction tx) throws IOException {
        return getRoot(tx).getMaxLeafDepth(tx, 0);
    }

    synchronized public void printStructure(Transaction tx, PrintWriter out) throws IOException {
        getRoot(tx).printStructure(tx, out, "");
    }
    
    synchronized public void printStructure(Transaction tx, OutputStream out) throws IOException {
        PrintWriter pw = new PrintWriter(out,false);
        getRoot(tx).printStructure(tx, pw, "");
        pw.flush();
    }

    synchronized public Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx) throws IOException {
        return getRoot(tx).iterator(tx);
    }
    
    synchronized public Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx, Key initialKey) throws IOException {
        return getRoot(tx).iterator(tx, initialKey);
    }
    
    synchronized public void visit(Transaction tx, BTreeVisitor<Key, Value> visitor) throws IOException {
        getRoot(tx).visit(tx, visitor);
    }

    synchronized public Map.Entry<Key,Value> getFirst(Transaction tx) throws IOException {
        return getRoot(tx).getFirst(tx);
    }

    synchronized public Map.Entry<Key,Value> getLast(Transaction tx) throws IOException {
        return getRoot(tx).getLast(tx);
    }

    ///////////////////////////////////////////////////////////////////
    // Internal implementation methods
    ///////////////////////////////////////////////////////////////////
    
    private void assertLoaded() throws IllegalStateException {
        if( !loaded.get() ) {
            throw new IllegalStateException("The BTreeIndex is not loaded");
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Internal methods made accessible to BTreeNode
    ///////////////////////////////////////////////////////////////////

    BTreeNode<Key,Value> loadNode(Transaction tx, long pageId, BTreeNode<Key,Value> parent) throws IOException {
        Page<BTreeNode<Key,Value>> page = tx.load(pageId, marshaller);
        BTreeNode<Key, Value> node = page.get();
        node.setPage(page);
        node.setParent(parent);
        return node;
    }

    BTreeNode<Key,Value> createNode(Transaction tx, BTreeNode<Key,Value> parent) throws IOException {
        Page<BTreeNode<Key,Value>> p = tx.allocate();
        BTreeNode<Key,Value> node = new BTreeNode<Key,Value>(this);
        node.setPage(p);
        node.setParent(parent);
        node.setEmpty();
        p.set(node);
        return node;
    }

    BTreeNode<Key,Value> createNode(Page<BTreeNode<Key,Value>> p, BTreeNode<Key,Value> parent) throws IOException {
        BTreeNode<Key,Value> node = new BTreeNode<Key,Value>(this);
        node.setPage(p);
        node.setParent(parent);
        node.setEmpty();
        p.set(node);
        return node;
    }
    
    void storeNode(Transaction tx, BTreeNode<Key,Value> node, boolean overflow) throws IOException {
        tx.store(node.getPage(), marshaller, overflow);
    }
        
   
    ///////////////////////////////////////////////////////////////////
    // Property Accessors
    ///////////////////////////////////////////////////////////////////

    public PageFile getPageFile() {
        return pageFile;
    }
    public long getPageId() {
        return pageId;
    }

    public Marshaller<Key> getKeyMarshaller() {
        return keyMarshaller;
    }
    public void setKeyMarshaller(Marshaller<Key> keyMarshaller) {
        this.keyMarshaller = keyMarshaller;
    }

    public Marshaller<Value> getValueMarshaller() {
        return valueMarshaller;
    }
    public void setValueMarshaller(Marshaller<Value> valueMarshaller) {
        this.valueMarshaller = valueMarshaller;
    }

    public Prefixer<Key> getPrefixer() {
        return prefixer;
    }
    public void setPrefixer(Prefixer<Key> prefixer) {
        this.prefixer = prefixer;
    }

    public void setPageFile(PageFile pageFile) {
        this.pageFile = pageFile;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

}
