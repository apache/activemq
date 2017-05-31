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
package org.apache.activemq.util;

/**
 * Provides a base class for you to extend when you want object to maintain a
 * doubly linked list to other objects without using a collection class.
 *
 * @author chirino
 */
public class LinkedNode {

    protected LinkedNode next = this;
    protected LinkedNode prev = this;
    protected boolean tail = true;

    public LinkedNode getHeadNode() {
        if (isHeadNode()) {
            return this;
        }
        if (isTailNode()) {
            return next;
        }
        LinkedNode rc = prev;
        while (!rc.isHeadNode()) {
            rc = rc.prev;
        }
        return rc;
    }

    public LinkedNode getTailNode() {
        if (isTailNode()) {
            return this;
        }
        if (isHeadNode()) {
            return prev;
        }
        LinkedNode rc = next;
        while (!rc.isTailNode()) {
            rc = rc.next;
        }
        return rc;
    }

    public LinkedNode getNext() {
        return tail ? null : next;
    }

    public LinkedNode getPrevious() {
        return prev.tail ? null : prev;
    }

    public boolean isHeadNode() {
        return prev.isTailNode();
    }

    public boolean isTailNode() {
        return tail;
    }

    /**
     * @param rightHead the node to link after this node.
     * @return this
     */
    public LinkedNode linkAfter(LinkedNode rightHead) {

        if (rightHead == this) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }
        if (!rightHead.isHeadNode()) {
            throw new IllegalArgumentException("You only insert nodes that are the first in a list");
        }

        LinkedNode rightTail = rightHead.prev;

        if (tail) {
            tail = false;
        } else {
            rightTail.tail = false;
        }

        rightHead.prev = this; // link the head of the right side.
        rightTail.next = next; // link the tail of the right side
        next.prev = rightTail; // link the head of the left side
        next = rightHead; // link the tail of the left side.

        return this;
    }

    /**
     * @param leftHead the node to link after this node.
     * @return this
     */
    public LinkedNode linkBefore(LinkedNode leftHead) {

        if (leftHead == this) {
            throw new IllegalArgumentException("You cannot link to yourself");
        }
        if (!leftHead.isHeadNode()) {
            throw new IllegalArgumentException("You only insert nodes that are the first in a list");
        }

        // The left side is no longer going to be a tail..
        LinkedNode leftTail = leftHead.prev;
        leftTail.tail = false;

        leftTail.next = this; // link the tail of the left side.
        leftHead.prev = prev; // link the head of the left side.
        prev.next = leftHead; // link the tail of the right side.
        prev = leftTail; // link the head of the right side.

        return leftHead;
    }

    /**
     * Removes this node out of the linked list it is chained in.
     */
    public void unlink() {
        // If we are allready unlinked...
        if (prev == this) {
            reset();
            return;
        }

        if (tail) {
            prev.tail = true;
        }

        // Update the peers links..
        next.prev = prev;
        prev.next = next;

        // Update our links..
        reset();
    }

    public void reset() {
        next = this;
        prev = this;
        tail = true;
    }
}
