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

/**
 * Provides a base class for you to extend when you want object to maintain a
 * doubly linked list to other objects without using a collection class.
 * 
 * @author chirino
 */
public class LinkedNode<T extends LinkedNode<T>> {

    protected LinkedNodeList<T> list;
    protected T next;
    protected T prev;

    public LinkedNode() {
    }

    @SuppressWarnings("unchecked")
    private T getThis() {
        return (T) this;
    }

    public T getHeadNode() {
        return list.head;
    }

    public T getTailNode() {
        return list.head.prev;
    }

    public T getNext() {
        return isTailNode() ? null : next;
    }

    public T getPrevious() {
        return isHeadNode() ? null : prev;
    }

    public T getNextCircular() {
        return next;
    }

    public T getPreviousCircular() {
        return prev;
    }

    public boolean isHeadNode() {
        return list.head == this;
    }

    public boolean isTailNode() {
        return list.head.prev == this;
    }

    /**
     * @param node
     *            the node to link after this node.
     * @return this
     */
    public void linkAfter(T node) {
        if (node == this) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }
        if (node.list != null) {
            throw new IllegalArgumentException("You only insert nodes that are not in a list");
        }
        if (list == null) {
            throw new IllegalArgumentException("This node is not yet in a list");
        }

        node.list = list;

        // given we linked this<->next and are inserting node in between
        node.prev = getThis(); // link this<-node
        node.next = next; // link node->next
        next.prev = node; // link node<-next
        next = node; // this->node
        list.size++;
    }

    /**
     * @param rightList
     *            the node to link after this node.
     * @return this
     */
    public void linkAfter(LinkedNodeList<T> rightList) {

        if (rightList == list) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }
        if (list == null) {
            throw new IllegalArgumentException("This node is not yet in a list");
        }

        T rightHead = rightList.head;
        T rightTail = rightList.head.prev;
        list.reparent(rightList);

        // given we linked this<->next and are inserting list in between
        rightHead.prev = getThis(); // link this<-list
        rightTail.next = next; // link list->next
        next.prev = rightTail; // link list<-next
        next = rightHead; // this->list
    }

    /**
     * @param node
     *            the node to link after this node.
     * @return
     * @return this
     */
    public void linkBefore(T node) {

        if (node == this) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }
        if (node.list != null) {
            throw new IllegalArgumentException("You only insert nodes that are not in a list");
        }
        if (list == null) {
            throw new IllegalArgumentException("This node is not yet in a list");
        }

        node.list = list;

        // given we linked prev<->this and are inserting node in between
        node.next = getThis(); // node->this
        node.prev = prev; // prev<-node
        prev.next = node; // prev->node
        prev = node; // node<-this

        if (this == list.head) {
            list.head = node;
        }
        list.size++;
    }

    /**
     * @param leftList
     *            the node to link after this node.
     * @return
     * @return this
     */
    public void linkBefore(LinkedNodeList<T> leftList) {

        if (leftList == list) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }
        if (list == null) {
            throw new IllegalArgumentException("This node is not yet in a list");
        }

        T leftHead = leftList.head;
        T leftTail = leftList.head.prev;
        list.reparent(leftList);

        // given we linked prev<->this and are inserting list in between
        leftTail.next = getThis(); // list->this
        leftHead.prev = prev; // prev<-list
        prev.next = leftHead; // prev->list
        prev = leftTail; // list<-this

        if (isHeadNode()) {
            list.head = leftHead;
        }
    }

    public void linkToTail(LinkedNodeList<T> target) {
        if (list != null) {
            throw new IllegalArgumentException("This node is already linked to a node");
        }

        if (target.head == null) {
            next = prev = target.head = getThis();
            list = target;
            list.size++;
        } else {
            target.head.prev.linkAfter(getThis());
        }
    }

    public void linkToHead(LinkedNodeList<T> target) {
        if (list != null) {
            throw new IllegalArgumentException("This node is already linked to a node");
        }

        if (target.head == null) {
            next = prev = target.head = getThis();
            list = target;
            list.size++;
        } else {
            target.head.linkBefore(getThis());
        }
    }

    /**
     * Removes this node out of the linked list it is chained in.
     */
    public boolean unlink() {

        // If we are allready unlinked...
        if (list == null) {
            return false;
        }

        if (getThis() == prev) {
            // We are the only item in the list
            list.head = null;
        } else {
            // given we linked prev<->this<->next
            next.prev = prev; // prev<-next
            prev.next = next; // prev->next

            if (isHeadNode()) {
                list.head = next;
            }
        }
        list.size--;
        list = null;
        return true;
    }

    /**
     * Splits the list into 2 lists. This node becomes the tail of this list.
     * Then 2nd list is returned.
     * 
     * @return An empty list if this is a tail node.
     */
    public LinkedNodeList<T> splitAfter() {

        if (isTailNode()) {
            return new LinkedNodeList<T>();
        }

        // Create the new list
        LinkedNodeList<T> newList = new LinkedNodeList<T>();
        newList.head = next;

        // Update the head and tail of the new list so that they point to each
        // other.
        newList.head.prev = list.head.prev; // new list: tail<-head
        newList.head.prev.next = newList.head; // new list: tail->head
        next = list.head; // old list: tail->head
        list.head.prev = getThis(); // old list: tail<-head

        // Update all the nodes in the new list so that they know of their new
        // list owner.
        T n = newList.head;
        newList.size++;
        list.size--;
        do {
            n.list = newList;
            n = n.next;
            newList.size++;
            list.size--;
        } while (n != newList.head);

        return newList;
    }

    /**
     * Splits the list into 2 lists. This node becomes the head of this list.
     * Then 2nd list is returned.
     * 
     * @return An empty list if this is a head node.
     */
    public LinkedNodeList<T> splitBefore() {

        if (isHeadNode()) {
            return new LinkedNodeList<T>();
        }

        // Create the new list
        LinkedNodeList<T> newList = new LinkedNodeList<T>();
        newList.head = list.head;
        list.head = getThis();

        T newListTail = prev;

        prev = newList.head.prev; // old list: tail<-head
        prev.next = getThis(); // old list: tail->head
        newList.head.prev = newListTail; // new list: tail<-head
        newListTail.next = newList.head; // new list: tail->head

        // Update all the nodes in the new list so that they know of their new
        // list owner.
        T n = newList.head;
        newList.size++;
        list.size--;
        do {
            n.list = newList;
            n = n.next;
            newList.size++;
            list.size--;
        } while (n != newList.head);

        return newList;
    }

    public boolean isLinked() {
        return list != null;
    }

	public LinkedNodeList<T> getList() {
		return list;
	}

}
