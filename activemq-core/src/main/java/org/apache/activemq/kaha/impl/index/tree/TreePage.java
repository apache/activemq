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
package org.apache.activemq.kaha.impl.index.tree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.kaha.Marshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Page in a BTree
 * 
 * @version $Revision: 1.1.1.1 $
 */
class TreePage {

    static final int PAGE_HEADER_SIZE = 18;
    private static final transient Logger LOG = LoggerFactory.getLogger(TreePage.class);

    static enum Flavour {
        LESS, MORE
    }

    private TreeIndex tree;
    private int maximumEntries;
    private long id;
    private long parentId = TreeEntry.NOT_SET;
    private boolean leaf = true;
    private List<TreeEntry> treeEntries;
    /*
     * for persistence only
     */
    private long nextFreePageId = TreeEntry.NOT_SET;
    private boolean active = true;

    /**
     * Constructor
     * 
     * @param tree
     * @param id
     * @param parentId
     * @param maximumEntries
     */
    TreePage(TreeIndex tree, long id, long parentId, int maximumEntries) {
        this(maximumEntries);
        this.tree = tree;
        this.id = id;
        this.parentId = parentId;
    }

    /**
     * Constructor
     * 
     * @param maximumEntries
     */
    public TreePage(int maximumEntries) {
        this.maximumEntries = maximumEntries;
        this.treeEntries = new ArrayList<TreeEntry>(maximumEntries);
    }

    public String toString() {
        return "TreePage[" + getId() + "]parent=" + getParentId();
    }

    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof TreePage) {
            TreePage other = (TreePage)o;
            result = other.id == id;
        }
        return result;
    }

    public int hashCode() {
        return (int)id;
    }

    boolean isActive() {
        return this.active;
    }

    void setActive(boolean active) {
        this.active = active;
    }

    long getNextFreePageId() {
        return this.nextFreePageId;
    }

    void setNextFreePageId(long nextPageId) {
        this.nextFreePageId = nextPageId;
    }

    long getId() {
        return id;
    }

    void setId(long id) {
        this.id = id;
    }

    void write(Marshaller keyMarshaller, DataOutput dataOut) throws IOException {
        writeHeader(dataOut);
        dataOut.writeInt(treeEntries.size());
        for (TreeEntry entry : treeEntries) {
            entry.write(keyMarshaller, dataOut);
        }
    }

    void read(Marshaller keyMarshaller, DataInput dataIn) throws IOException {
        readHeader(dataIn);
        int size = dataIn.readInt();
        treeEntries.clear();
        for (int i = 0; i < size; i++) {
            TreeEntry entry = new TreeEntry();
            entry.read(keyMarshaller, dataIn);
            treeEntries.add(entry);
        }
    }

    void readHeader(DataInput dataIn) throws IOException {
        active = dataIn.readBoolean();
        leaf = dataIn.readBoolean();
        setParentId(dataIn.readLong());
        nextFreePageId = dataIn.readLong();
    }

    void writeHeader(DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(isActive());
        dataOut.writeBoolean(isLeaf());
        dataOut.writeLong(getParentId());
        dataOut.writeLong(nextFreePageId);
    }

    boolean isEmpty() {
        return treeEntries.isEmpty();
    }

    boolean isFull() {
        return treeEntries.size() >= maximumEntries;
    }

    boolean isRoot() {
        return getParentId() < 0;
    }

    boolean isLeaf() {
        if (treeEntries.isEmpty()) {
            leaf = true;
        }
        return leaf;
    }

    boolean isUnderflowed() {
        return treeEntries.size() < (maximumEntries / 2);
    }

    boolean isOverflowed() {
        return treeEntries.size() > maximumEntries;
    }

    void setLeaf(boolean newValue) {
        this.leaf = newValue;
    }

    TreePage getParent() throws IOException {
        return tree.lookupPage(parentId);
    }

    long getParentId() {
        return parentId;
    }

    void setParentId(long newId) throws IOException {
        if (newId == this.id) {
            throw new IllegalStateException("Cannot set page as a child of itself " + this
                                            + " trying to set parentId = " + newId);
        }
        this.parentId = newId;
        tree.writePage(this);
    }

    List<TreeEntry> getEntries() {
        return treeEntries;
    }

    void setEntries(List<TreeEntry> newEntries) {
        this.treeEntries = newEntries;
    }

    int getMaximumEntries() {
        return this.maximumEntries;
    }

    void setMaximumEntries(int maximumEntries) {
        this.maximumEntries = maximumEntries;
    }

    int size() {
        return treeEntries.size();
    }

    TreeIndex getTree() {
        return this.tree;
    }

    void setTree(TreeIndex tree) {
        this.tree = tree;
    }

    void reset() throws IOException {
        treeEntries.clear();
        setParentId(TreeEntry.NOT_SET);
        setNextFreePageId(TreeEntry.NOT_SET);
        setLeaf(true);
    }

    public TreeEntry find(TreeEntry key) throws IOException {
        int low = 0;
        int high = size() - 1;
        long pageId = -1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            TreeEntry te = getTreeEntry(mid);
            int cmp = te.compareTo(key);
            if (cmp == 0) {
                return te;
            } else if (cmp < 0) {
                low = mid + 1;
                pageId = te.getNextPageId();
            } else {
                high = mid - 1;
                pageId = te.getPrevPageId();
            }
        }
        TreePage page = tree.lookupPage(pageId);
        if (page != null) {
            return page.find(key);
        }
        return null;
    }

    TreeEntry put(TreeEntry newEntry) throws IOException {
        TreeEntry result = null;
        if (isRoot()) {
            if (isEmpty()) {
                insertTreeEntry(0, newEntry);
            } else {
                result = doInsert(null, newEntry);
            }
        } else {
            throw new IllegalStateException("insert() should not be called on non root page - " + this);
        }
        return result;
    }

    TreeEntry remove(TreeEntry entry) throws IOException {
        TreeEntry result = null;
        if (isRoot()) {
            if (!isEmpty()) {
                result = doRemove(entry);
            }
        } else {
            throw new IllegalStateException("remove() should not be called on non root page");
        }
        return result;
    }

    private TreeEntry doInsert(Flavour flavour, TreeEntry newEntry) throws IOException {
        TreeEntry result = null;
        TreePageEntry closest = findClosestEntry(newEntry);
        if (closest != null) {
            TreeEntry closestEntry = closest.getTreeEntry();
            TreePage closestPage = closest.getTreePage();
            int cmp = closestEntry.compareTo(newEntry);
            if (cmp == 0) {
                // we actually just need to pass back the value
                long oldValue = closestEntry.getIndexOffset();
                closestEntry.setIndexOffset(newEntry.getIndexOffset());
                newEntry.setIndexOffset(oldValue);
                result = newEntry;
                save();
            } else if (closestPage != null) {
                result = closestPage.doInsert(closest.getFlavour(), newEntry);
            } else {
                if (!isFull()) {
                    insertTreeEntry(closest.getIndex(), newEntry);
                    save();
                } else {
                    doOverflow(flavour, newEntry);
                }
            }
        } else {
            if (!isFull()) {
                doInsertEntry(newEntry);
                save();
            } else {
                // need to insert the new entry and propogate up the hightest
                // value
                doOverflow(flavour, newEntry);
            }
        }
        return result;
    }

    private TreePage doOverflow(Flavour flavour, TreeEntry newEntry) throws IOException {
        TreePage result = this;
        TreeEntry theEntry = newEntry;
        if (!isFull()) {
            doInsertEntry(newEntry);
            save();
        } else {
            if (!isRoot() && flavour != null) {
                // we aren't the root, but to ensure the correct distribution we
                // need to
                // insert the new entry and take a node of the end of the page
                // and pass that up the tree to find a home
                doInsertEntry(newEntry);
                if (flavour == Flavour.LESS) {
                    theEntry = removeTreeEntry(0);
                    theEntry.reset();
                    theEntry.setNextPageId(getId());
                } else {
                    theEntry = removeTreeEntry(size() - 1);
                    theEntry.reset();
                    theEntry.setPrevPageId(getId());
                }
                save();

                result = getParent().doOverflow(flavour, theEntry);
                if (!theEntry.equals(newEntry)) {
                    // the newEntry stayed here
                    result = this;
                }
            } else {
                // so we are the root and need to split
                doInsertEntry(newEntry);
                int midIndex = size() / 2;
                TreeEntry midEntry = removeTreeEntry(midIndex);
                List<TreeEntry> subList = getSubList(midIndex, size());
                removeAllTreeEntries(subList);
                TreePage newRoot = tree.createRoot();
                newRoot.setLeaf(false);
                this.setParentId(newRoot.getId());
                save(); // we are no longer root - need to save - we maybe
                // looked up v. soon!
                TreePage rightPage = tree.createPage(newRoot.getId());
                rightPage.setEntries(subList);
                rightPage.checkLeaf();
                resetParentId(rightPage.getId(), rightPage.getEntries());
                midEntry.setNextPageId(rightPage.getId());
                midEntry.setPrevPageId(this.getId());
                newRoot.insertTreeEntry(0, midEntry);
                resetParentId(newRoot.getId(), newRoot.getEntries());
                save();
                rightPage.save();
                newRoot.save();
            }
        }
        return result;
    }

    private TreeEntry doRemove(TreeEntry entry) throws IOException {
        TreeEntry result = null;
        TreePageEntry closest = findClosestEntry(entry);
        if (closest != null) {
            TreeEntry closestEntry = closest.getTreeEntry();
            if (closestEntry != null) {
                TreePage closestPage = closest.getTreePage();
                int cmp = closestEntry.compareTo(entry);
                if (cmp == 0) {
                    result = closest.getTreeEntry();
                    int index = closest.getIndex();
                    removeTreeEntry(index);
                    save();
                    // ensure we don't loose children
                    doUnderflow(result, index);
                } else if (closestPage != null) {
                    closestPage.doRemove(entry);
                }
            }
        }
        return result;
    }

    /**
     * @return true if the page is removed
     * @throws IOException
     */
    private boolean doUnderflow() throws IOException {
        boolean result = false;
        boolean working = true;
        while (working && isUnderflowed() && !isEmpty() && !isLeaf()) {
            int lastIndex = size() - 1;
            TreeEntry entry = getTreeEntry(lastIndex);
            working = doUnderflow(entry, lastIndex);
        }
        if (isUnderflowed() && isLeaf()) {
            result = doUnderflowLeaf();
        }
        return result;
    }

    private boolean doUnderflow(TreeEntry entry, int index) throws IOException {
        boolean result = false;
        // pull an entry up from a leaf to fill the empty space
        if (entry.getNextPageId() != TreeEntry.NOT_SET) {
            TreePage page = tree.lookupPage(entry.getNextPageId());
            if (page != null && !page.isEmpty()) {
                TreeEntry replacement = page.removeTreeEntry(0);
                TreeEntry copy = replacement.copy();
                checkParentIdForRemovedPageEntry(copy, page.getId(), getId());
                if (!page.isEmpty()) {
                    copy.setNextPageId(page.getId());
                    page.setParentId(this.id);
                } else {
                    page.setLeaf(true);
                }
                int replacementIndex = doInsertEntry(copy);
                if (page.doUnderflow()) {
                    // page removed so update our replacement
                    resetPageReference(replacementIndex, copy.getNextPageId());
                    copy.setNextPageId(TreeEntry.NOT_SET);
                } else {
                    page.save();
                }
                save();
                result = true;
            }
        }
        // ensure we don't loose previous bit of the tree
        if (entry.getPrevPageId() != TreeEntry.NOT_SET) {
            TreeEntry prevEntry = (index > 0) ? getTreeEntry(index - 1) : null;
            if (prevEntry == null || prevEntry.getNextPageId() != entry.getPrevPageId()) {
                TreePage page = tree.lookupPage(entry.getPrevPageId());
                if (page != null && !page.isEmpty()) {
                    TreeEntry replacement = page.removeTreeEntry(page.getEntries().size() - 1);
                    TreeEntry copy = replacement.copy();
                    // check children pages of the replacement point to the
                    // correct place
                    checkParentIdForRemovedPageEntry(copy, page.getId(), getId());
                    if (!page.isEmpty()) {
                        copy.setPrevPageId(page.getId());
                    } else {
                        page.setLeaf(true);
                    }
                    insertTreeEntry(index, copy);
                    // if we overflow - the page the replacement ends up on
                    TreePage landed = null;
                    TreeEntry removed = null;
                    if (isOverflowed()) {
                        TreePage parent = getParent();
                        if (parent != null) {
                            removed = getTreeEntry(0);
                            Flavour flavour = getFlavour(parent, removed);
                            if (flavour == Flavour.LESS) {
                                removed = removeTreeEntry(0);
                                landed = parent.doOverflow(flavour, removed);
                            } else {
                                removed = removeTreeEntry(size() - 1);
                                landed = parent.doOverflow(Flavour.MORE, removed);
                            }
                        }
                    }
                    if (page.doUnderflow()) {
                        if (landed == null || landed.equals(this)) {
                            landed = this;
                        }

                        resetPageReference(copy.getNextPageId());
                        landed.resetPageReference(copy.getNextPageId());
                        copy.setPrevPageId(TreeEntry.NOT_SET);
                        landed.save();
                    } else {
                        page.save();
                    }
                    save();
                    result = true;
                }
                // now we need to check we haven't overflowed this page
            }
        }
        if (!result) {
            save();
        }
        // now see if we need to save this page
        result |= doUnderflowLeaf();
        save();
        return result;
    }

    private boolean doUnderflowLeaf() throws IOException {
        boolean result = false;
        // if we have unerflowed - and we are a leaf - push entries further up
        // the tree
        // and delete ourselves
        if (isUnderflowed() && isLeaf()) {
            List<TreeEntry> list = new ArrayList<TreeEntry>(treeEntries);
            treeEntries.clear();
            for (TreeEntry entry : list) {
                // need to check for each iteration - we might get promoted to
                // root
                TreePage parent = getParent();
                if (parent != null) {
                    Flavour flavour = getFlavour(parent, entry);
                    TreePage landedOn = parent.doOverflow(flavour, entry);
                    checkParentIdForRemovedPageEntry(entry, getId(), landedOn.getId());
                }
            }
            TreePage parent = getParent();
            if (parent != null) {
                parent.checkLeaf();
                parent.removePageId(getId());
                parent.doUnderflow();
                parent.save();
                tree.releasePage(this);
                result = true;
            }
        }
        return result;
    }

    private Flavour getFlavour(TreePage page, TreeEntry entry) {
        Flavour result = null;
        if (page != null && !page.getEntries().isEmpty()) {
            TreeEntry last = page.getEntries().get(page.getEntries().size() - 1);
            if (last.compareTo(entry) > 0) {
                result = Flavour.MORE;
            } else {
                result = Flavour.LESS;
            }
        }
        return result;
    }

    private void checkLeaf() {
        boolean result = false;
        for (TreeEntry entry : treeEntries) {
            if (entry.hasChildPagesReferences()) {
                result = true;
                break;
            }
        }
        setLeaf(!result);
    }

    private void checkParentIdForRemovedPageEntry(TreeEntry entry, long oldPageId, long newPageId)
        throws IOException {
        TreePage page = tree.lookupPage(entry.getPrevPageId());
        if (page != null && page.getParentId() == oldPageId) {
            page.setParentId(newPageId);
            page.save();
        }
        page = tree.lookupPage(entry.getNextPageId());
        if (page != null && page.getParentId() == oldPageId) {
            page.setParentId(newPageId);
            page.save();
        }
    }

    private void removePageId(long pageId) {
        for (TreeEntry entry : treeEntries) {
            if (entry.getNextPageId() == pageId) {
                entry.setNextPageId(TreeEntry.NOT_SET);
            }
            if (entry.getPrevPageId() == pageId) {
                entry.setPrevPageId(TreeEntry.NOT_SET);
            }
        }
    }

    private TreePageEntry findClosestEntry(TreeEntry key) throws IOException {
        TreePageEntry result = null;
        TreeEntry treeEntry = null;
        Flavour flavour = null;
        long pageId = -1;
        int low = 0;
        int high = size() - 1;
        int mid = low;
        while (low <= high) {
            mid = (low + high) >> 1;
            treeEntry = getTreeEntry(mid);
            int cmp = treeEntry.compareTo(key);
            if (cmp < 0) {
                low = mid + 1;
                pageId = treeEntry.getNextPageId();
                flavour = Flavour.LESS;
            } else if (cmp > 0) {
                high = mid - 1;
                pageId = treeEntry.getPrevPageId();
                flavour = Flavour.MORE;
            } else {
                // got exact match
                low = mid;
                break;
            }
        }
        if (treeEntry != null) {
            TreePage treePage = tree.lookupPage(pageId);
            result = new TreePageEntry(treeEntry, treePage, flavour, low);
        }
        return result;
    }

    private int doInsertEntry(TreeEntry newEntry) throws IOException {
        int low = 0;
        int high = size() - 1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            TreeEntry midVal = getTreeEntry(mid);
            int cmp = midVal.compareTo(newEntry);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            }
        }
        insertTreeEntry(low, newEntry);
        return low;
    }

    private void insertTreeEntry(int index, TreeEntry entry) throws IOException {
        int p = index - 1;
        int n = index;
        TreeEntry prevEntry = (p >= 0 && p < treeEntries.size()) ? treeEntries.get(p) : null;
        TreeEntry nextEntry = (n >= 0 && n < treeEntries.size()) ? treeEntries.get(n) : null;
        if (prevEntry != null) {
            if (prevEntry.getNextPageId() == entry.getNextPageId()) {
                prevEntry.setNextPageId(TreeEntry.NOT_SET);
            }
            if (entry.getPrevPageId() == TreeEntry.NOT_SET) {
                entry.setPrevPageId(prevEntry.getNextPageId());
            }
        }
        if (nextEntry != null) {
            if (nextEntry.getPrevPageId() == entry.getPrevPageId()) {
                nextEntry.setPrevPageId(TreeEntry.NOT_SET);
            }
            if (entry.getNextPageId() == TreeEntry.NOT_SET) {
                entry.setNextPageId(nextEntry.getPrevPageId());
            }
        }
        addTreeEntry(index, entry);
    }

    private void resetPageReference(int index, long pageId) {
        int p = index - 1;
        int n = index;
        TreeEntry prevEntry = (p >= 0 && p < treeEntries.size()) ? treeEntries.get(p) : null;
        TreeEntry nextEntry = (n >= 0 && n < treeEntries.size()) ? treeEntries.get(n) : null;
        if (prevEntry != null) {
            if (prevEntry.getNextPageId() == pageId) {
                prevEntry.setNextPageId(TreeEntry.NOT_SET);
            }
        }
        if (nextEntry != null) {
            if (nextEntry.getPrevPageId() == pageId) {
                nextEntry.setPrevPageId(TreeEntry.NOT_SET);
            }
        }
    }

    private boolean resetPageReference(long pageId) {
        boolean updated = false;
        for (TreeEntry entry : treeEntries) {
            if (entry.getPrevPageId() == pageId) {
                entry.setPrevPageId(TreeEntry.NOT_SET);
                updated = true;
            }
            if (entry.getNextPageId() == pageId) {
                entry.setNextPageId(TreeEntry.NOT_SET);
                updated = true;
            }
        }
        return updated;
    }

    private void resetParentId(long newParentId, List<TreeEntry> entries) throws IOException {
        Set<Long> set = new HashSet<Long>();
        for (TreeEntry entry : entries) {
            if (entry != null) {
                set.add(entry.getPrevPageId());
                set.add(entry.getNextPageId());
            }
        }
        for (Long pageId : set) {
            TreePage page = tree.lookupPage(pageId);
            if (page != null) {
                page.setParentId(newParentId);
            }
        }
    }

    private void addTreeEntry(int index, TreeEntry entry) throws IOException {
        treeEntries.add(index, entry);
    }

    private TreeEntry removeTreeEntry(int index) throws IOException {
        TreeEntry result = treeEntries.remove(index);
        return result;
    }

    private void removeAllTreeEntries(List<TreeEntry> c) {
        treeEntries.removeAll(c);
    }

    private List<TreeEntry> getSubList(int from, int to) {
        return new ArrayList<TreeEntry>(treeEntries.subList(from, to));
    }

    private TreeEntry getTreeEntry(int index) {
        TreeEntry result = treeEntries.get(index);
        return result;
    }

    void saveHeader() throws IOException {
        tree.writePage(this);
    }

    void save() throws IOException {
        tree.writeFullPage(this);
    }

    protected void dump() throws IOException {
        LOG.info(this.toString());
        Set<Long> set = new HashSet<Long>();
        for (TreeEntry entry : treeEntries) {
            if (entry != null) {
                LOG.info(entry.toString());
                set.add(entry.getPrevPageId());
                set.add(entry.getNextPageId());
            }
        }
        for (Long pageId : set) {
            TreePage page = tree.lookupPage(pageId);
            if (page != null) {
                page.dump();
            }
        }
    }
}
