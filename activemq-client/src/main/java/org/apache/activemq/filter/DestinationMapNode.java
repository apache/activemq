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
package org.apache.activemq.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation class used to implement {@link DestinationMap}
 *
 *
 */
public class DestinationMapNode implements DestinationNode {
    protected static final String ANY_CHILD = DestinationMap.ANY_CHILD;
    protected static final String ANY_DESCENDENT = DestinationMap.ANY_DESCENDENT;

    // we synchronize at the DestinationMap level
    private DestinationMapNode parent;
    private List<Object> values = new ArrayList<Object>();
    private Map<String, DestinationNode> childNodes = new HashMap<String, DestinationNode>();
    private String path = "Root";
    // private DestinationMapNode anyChild;
    private int pathLength;

    public DestinationMapNode(DestinationMapNode parent) {
        this.parent = parent;
        if (parent == null) {
            pathLength = 0;
        } else {
            pathLength = parent.pathLength + 1;
        }
    }

    /**
     * Returns the child node for the given named path or null if it does not
     * exist
     */
    public DestinationNode getChild(String path) {
        return childNodes.get(path);
    }

    /**
     * Returns the child nodes
     */
    public Collection<DestinationNode> getChildren() {
        return childNodes.values();
    }

    public int getChildCount() {
        return childNodes.size();
    }

    /**
     * Returns the child node for the given named path, lazily creating one if
     * it does not yet exist
     */
    public DestinationMapNode getChildOrCreate(String path) {
        DestinationMapNode answer = (DestinationMapNode)childNodes.get(path);
        if (answer == null) {
            answer = createChildNode();
            answer.path = path;
            childNodes.put(path, answer);
        }
        return answer;
    }

    /**
     * Returns a mutable List of the values available at this node in the tree
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List getValues() {
        return values;
    }

    /**
     * Removes values available at this node in the tree
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List removeValues() {
        ArrayList v = new ArrayList(values);
        // parent.getAnyChildNode().getValues().removeAll(v);
        values.clear();
        pruneIfEmpty();
        return v;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Set removeDesendentValues() {
        Set answer = new HashSet();
        removeDesendentValues(answer);
        return answer;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected void removeDesendentValues(Set answer) {
        ArrayList<DestinationNode> candidates = new ArrayList<>();
        for (Map.Entry<String, DestinationNode> child : childNodes.entrySet()) {
            candidates.add(child.getValue());
        }

        for (DestinationNode node : candidates) {
            // remove all the values from the child
            answer.addAll(node.removeValues());
            answer.addAll(node.removeDesendentValues());
        }
    }

    /**
     * Returns a list of all the values from this node down the tree
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Set getDesendentValues() {
        Set answer = new HashSet();
        appendDescendantValues(answer);
        return answer;
    }

    public void add(String[] paths, int idx, Object value) {
        if (idx >= paths.length) {
            values.add(value);
        } else {
            getChildOrCreate(paths[idx]).add(paths, idx + 1, value);
        }
    }

    public void set(String[] paths, int idx, Object value) {
        if (idx >= paths.length) {
            values.clear();
            values.add(value);
        } else {
            getChildOrCreate(paths[idx]).set(paths, idx + 1, value);
        }
    }

    public void remove(String[] paths, int idx, Object value) {
        if (idx >= paths.length) {
            values.remove(value);
            pruneIfEmpty();
        } else {
            getChildOrCreate(paths[idx]).remove(paths, ++idx, value);
        }
    }

    public void removeAll(Set<DestinationNode> answer, String[] paths, int startIndex) {
        DestinationNode node = this;
        int size = paths.length;
        for (int i = startIndex; i < size && node != null; i++) {

            String path = paths[i];
            if (path.equals(ANY_DESCENDENT)) {
                answer.addAll(node.removeDesendentValues());
                break;
            }

            // TODO is this correct, we are appending wildcard values here???
            node.appendMatchingWildcards(answer, paths, i);
            if (path.equals(ANY_CHILD)) {
                // node = node.getAnyChildNode();
                node = new AnyChildDestinationNode(node);
            } else {
                node = node.getChild(path);
            }
        }

        if (node != null) {
            answer.addAll(node.removeValues());
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void appendDescendantValues(Set answer) {
        // add children values, then recursively add their children
        for(DestinationNode child : childNodes.values()) {
            answer.addAll(child.getValues());
            child.appendDescendantValues(answer);
        }
    }

    /**
     * Factory method to create a child node
     */
    protected DestinationMapNode createChildNode() {
        return new DestinationMapNode(this);
    }

    /**
     * Matches any entries in the map containing wildcards
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void appendMatchingWildcards(Set answer, String[] paths, int idx) {
        if (idx - 1 > pathLength) {
            return;
        }
        DestinationNode wildCardNode = getChild(ANY_CHILD);
        if (wildCardNode != null) {
            wildCardNode.appendMatchingValues(answer, paths, idx + 1);
        }
        wildCardNode = getChild(ANY_DESCENDENT);
        if (wildCardNode != null) {
            // for a wildcard Node match, add all values of the descendant node
            answer.addAll(wildCardNode.getValues());
            // and all descendants for paths like ">.>"
            answer.addAll(wildCardNode.getDesendentValues());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void appendMatchingValues(Set answer, String[] paths, int idx) {
        appendMatchingValues(answer, paths, idx, true);
    }

    public void appendMatchingValues(Set<DestinationNode> answer, String[] paths, int startIndex, boolean deep) {
        DestinationNode node = this;
        boolean couldMatchAny = true;
        int size = paths.length;
        for (int i = startIndex; i < size && node != null; i++) {
            String path = paths[i];
            if (deep && path != null && path.equals(ANY_DESCENDENT)) {
                answer.addAll(node.getDesendentValues());
                couldMatchAny = false;
                break;
            }

            node.appendMatchingWildcards(answer, paths, i);

            if (path.equals(ANY_CHILD)) {
                node = new AnyChildDestinationNode(node);
            } else {
                node = node.getChild(path);
            }
        }
        if (node != null) {
            answer.addAll(node.getValues());
            if (couldMatchAny) {
                // lets allow FOO.BAR to match the FOO.BAR.> entry in the map
                DestinationNode child = node.getChild(ANY_DESCENDENT);
                if (child != null) {
                    answer.addAll(child.getValues());
                }
            }
        }
    }

    public String getPath() {
        return path;
    }

    public boolean isEmpty(){
        return childNodes.isEmpty();
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
