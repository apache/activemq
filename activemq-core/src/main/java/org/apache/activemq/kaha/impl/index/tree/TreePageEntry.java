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

/**
 * A conglomarate used for return results from a tree lookup
 * 
 * @version $Revision: 1.1.1.1 $
 */
class TreePageEntry {

    private TreeEntry treeEntry;
    private TreePage treePage;
    private TreePage.Flavour flavour;
    private int index = -1;

    TreePageEntry(TreeEntry treeEntry, TreePage treePage, TreePage.Flavour flavour, int index) {
        this.treeEntry = treeEntry;
        this.treePage = treePage;
        this.flavour = flavour;
        this.index = index;
    }

    /**
     * @return the flavour
     */
    TreePage.Flavour getFlavour() {
        return this.flavour;
    }

    /**
     * @param flavour the flavour to set
     */
    void setFlavour(TreePage.Flavour flavour) {
        this.flavour = flavour;
    }

    /**
     * @return the treePage
     */
    TreePage getTreePage() {
        return this.treePage;
    }

    /**
     * @param treePage the treePage to set
     */
    void setTreePage(TreePage treePage) {
        this.treePage = treePage;
    }

    /**
     * @return the index
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * @param index the index to set
     */
    public void setIndex(int index) {
        this.index = index;
    }

    /**
     * @return the treeEntry
     */
    public TreeEntry getTreeEntry() {
        return this.treeEntry;
    }

    /**
     * @param treeEntry the treeEntry to set
     */
    public void setTreeEntry(TreeEntry treeEntry) {
        this.treeEntry = treeEntry;
    }
}
