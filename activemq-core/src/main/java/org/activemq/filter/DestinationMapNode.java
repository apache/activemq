/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation class used to implement {@link DestinationMap}
 *
 * @version $Revision: 1.2 $
 */
public class DestinationMapNode {
    // we synchornize at the DestinationMap level
    private DestinationMapNode parent;
    private List values = new ArrayList();
    private Map childNodes = new HashMap();
    private String path = "*";
    private DestinationMapNode anyChild;
    protected static final String ANY_CHILD = DestinationMap.ANY_CHILD;
    protected static final String ANY_DESCENDENT = DestinationMap.ANY_DESCENDENT;


    public DestinationMapNode(DestinationMapNode parent) {
        this.parent = parent;
    }
    

    /**
     * Returns the child node for the given named path or null if it does not exist
     */
    public DestinationMapNode getChild(String path) {
        return (DestinationMapNode) childNodes.get(path);
    }

    public int getChildCount() {
        return childNodes.size();
    }
    
    /**
     * Returns the child node for the given named path, lazily creating one if it does
     * not yet exist
     */
    public DestinationMapNode getChildOrCreate(String path) {
        DestinationMapNode answer = (DestinationMapNode) childNodes.get(path);
        if (answer == null) {
            answer = createChildNode();
            answer.path = path;
            childNodes.put(path, answer);
        }
        return answer;
    }

    /**
     * Returns the node which represents all children (i.e. the * node)
     */
    public DestinationMapNode getAnyChildNode() {
        if (anyChild == null) {
            anyChild = createChildNode();
        }
        return anyChild;
    }

    /**
     * Returns a mutable List of the values available at this node in the tree
     */
    public List getValues() {
        return values;
    }

    /**
     * Returns a list of all the values from this node down the tree
     */
    public Set getDesendentValues() {
        Set answer = new HashSet();
        appendDescendantValues(answer);
        return answer;
    }

    public void add(String[] paths, int idx, Object value) {
        if (idx >= paths.length) {
            values.add(value);
        }
        else {
            if (idx == paths.length - 1) {
                getAnyChildNode().getValues().add(value);
            }
            else {
                getAnyChildNode().add(paths, idx + 1, value);
            }
            getChildOrCreate(paths[idx]).add(paths, idx + 1, value);
        }
    }

    public void remove(String[] paths, int idx, Object value) {
        if (idx >= paths.length) {
            values.remove(value);
            pruneIfEmpty();
        }
        else {
            if (idx == paths.length - 1) {
                getAnyChildNode().getValues().remove(value);
            }
            else {
                getAnyChildNode().remove(paths, idx + 1, value);
            }
            getChildOrCreate(paths[idx]).remove(paths, ++idx, value);
        }
    }

    public void removeAll(String[] paths, int idx) {
        if (idx >= paths.length) {
            values.clear();
            pruneIfEmpty();
        }
        else {
            if (idx == paths.length - 1) {
                getAnyChildNode().getValues().clear();
            }
            else {
                getAnyChildNode().removeAll(paths, idx + 1);
            }
            getChildOrCreate(paths[idx]).removeAll(paths, ++idx);
        }
    }

    protected void appendDescendantValues(Set answer) {
        answer.addAll(values);
        if (anyChild != null) {
            anyChild.appendDescendantValues(answer);
        }
    }

    /**
     * Factory method to create a child node
     */
    protected DestinationMapNode createChildNode() {
        return new DestinationMapNode(this);
    }

    public void appendMatchingWildcards(Set answer, String[] paths, int idx) {
        DestinationMapNode wildCardNode = getChild(ANY_CHILD);
        if (wildCardNode != null) {
            wildCardNode.appendMatchingValues(answer, paths, idx + 1);
        }
        wildCardNode = getChild(ANY_DESCENDENT);
        if (wildCardNode != null) {
            answer.addAll(wildCardNode.getDesendentValues());
        }
    }

    public void appendMatchingValues(Set answer, String[] paths, int startIndex) {
        DestinationMapNode node = this;
        for (int i = startIndex, size = paths.length; i < size && node != null; i++) {
            String path = paths[i];
            if (path.equals(ANY_DESCENDENT)) {
                answer.addAll(node.getDesendentValues());
                break;
            }

            node.appendMatchingWildcards(answer, paths, i);
            if (path.equals(ANY_CHILD)) {
                node = node.getAnyChildNode();
            }
            else {
                node = node.getChild(path);
            }
        }
        if (node != null) {
            answer.addAll(node.getValues());
        }
    }


    public String getPath() {
        return path;
    }

    protected void pruneIfEmpty() {
        if (parent != null && childNodes.isEmpty() && values.isEmpty()) {
            parent.removeChild(this);
        }
    }


    protected void removeChild(DestinationMapNode node) {
        childNodes.remove(node.getPath());
        pruneIfEmpty();
    }
}
