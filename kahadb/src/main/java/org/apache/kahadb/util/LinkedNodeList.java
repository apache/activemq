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
package org.apache.kahadb.util;

import java.util.ArrayList;

/**
 * Provides a list of LinkedNode objects. 
 * 
 * @author chirino
 */
public class LinkedNodeList<T extends LinkedNode<T>> {

    T head;
    int size;

    public LinkedNodeList() {
    }

    public boolean isEmpty() {
        return head == null;
    }

    public void addLast(T node) {
        node.linkToTail(this);
    }

    public void addFirst(T node) {
        node.linkToHead(this);
    }

    public T getHead() {
        return head;
    }

    public T getTail() {
        return head.prev;
    }
    
    public void clear() {
        while (head != null) {
            head.unlink();
        }
    }

    public void addLast(LinkedNodeList<T> list) {
        if (list.isEmpty()) {
            return;
        }
        if (head == null) {
            head = list.head;
            reparent(list);
        } else {
            getTail().linkAfter(list);
        }
    }

    public void addFirst(LinkedNodeList<T> list) {
        if (list.isEmpty()) {
            return;
        }
        if (head == null) {
            reparent(list);
            head = list.head;
            list.head = null;
        } else {
            getHead().linkBefore(list);
        }
    }

    public T reparent(LinkedNodeList<T> list) {
        size += list.size;
        T n = list.head;
        do {
            n.list = this;
            n = n.next;
        } while (n != list.head);
        list.head = null;
        list.size = 0;
        return n;
    }

    /**
     * Move the head to the tail and returns the new head node.
     * 
     * @return
     */
    public T rotate() {
        return head = head.getNextCircular();
    }

    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first=true;
        T cur = getHead();
        while( cur!=null ) {
            if( !first ) {
                sb.append(", ");
            }
            sb.append(cur);
            first=false;
            cur = cur.getNext();
        }
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Copies the nodes of the LinkedNodeList to an ArrayList.
     * @return
     */
    public ArrayList<T> toArrayList() {
    	ArrayList<T> rc = new ArrayList<T>(size);
    	T cur = head;
    	while( cur!=null ) {
    		rc.add(cur);
    		cur = cur.getNext();
    	}
    	return rc;
    }
}
