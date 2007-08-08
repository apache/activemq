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

import junit.framework.TestCase;

/**
 * @author chirino
 */
public class LinkedNodeTest extends TestCase {

    static class IntLinkedNode extends LinkedNode {
        public final int v;

        public IntLinkedNode(int v) {
            this.v = v;
        };

        @Override
        public String toString() {
            return "" + v;
        }
    }

    IntLinkedNode i1 = new IntLinkedNode(1);
    IntLinkedNode i2 = new IntLinkedNode(2);
    IntLinkedNode i3 = new IntLinkedNode(3);
    IntLinkedNode i4 = new IntLinkedNode(4);
    IntLinkedNode i5 = new IntLinkedNode(5);
    IntLinkedNode i6 = new IntLinkedNode(6);

    public void testLinkAfter() {

        i1.linkAfter(i2.linkAfter(i3));
        // Order should be 1,2,3

        assertTrue(i1.getNext() == i2);
        assertTrue(i1.getNext().getNext() == i3);
        assertNull(i1.getNext().getNext().getNext());

        assertTrue(i3.getPrevious() == i2);
        assertTrue(i3.getPrevious().getPrevious() == i1);
        assertNull(i3.getPrevious().getPrevious().getPrevious());

        assertTrue(i1.isHeadNode());
        assertFalse(i1.isTailNode());
        assertFalse(i2.isHeadNode());
        assertFalse(i2.isTailNode());
        assertTrue(i3.isTailNode());
        assertFalse(i3.isHeadNode());

        i1.linkAfter(i4.linkAfter(i5));

        // Order should be 1,4,5,2,3

        assertTrue(i1.getNext() == i4);
        assertTrue(i1.getNext().getNext() == i5);
        assertTrue(i1.getNext().getNext().getNext() == i2);
        assertTrue(i1.getNext().getNext().getNext().getNext() == i3);
        assertNull(i1.getNext().getNext().getNext().getNext().getNext());

        assertTrue(i3.getPrevious() == i2);
        assertTrue(i3.getPrevious().getPrevious() == i5);
        assertTrue(i3.getPrevious().getPrevious().getPrevious() == i4);
        assertTrue(i3.getPrevious().getPrevious().getPrevious().getPrevious() == i1);
        assertNull(i3.getPrevious().getPrevious().getPrevious().getPrevious().getPrevious());

        assertTrue(i1.isHeadNode());
        assertFalse(i1.isTailNode());
        assertFalse(i4.isHeadNode());
        assertFalse(i4.isTailNode());
        assertFalse(i5.isHeadNode());
        assertFalse(i5.isTailNode());
        assertFalse(i2.isHeadNode());
        assertFalse(i2.isTailNode());
        assertTrue(i3.isTailNode());
        assertFalse(i3.isHeadNode());

    }

    public void testLinkBefore() {

        i3.linkBefore(i2.linkBefore(i1));

        assertTrue(i1.getNext() == i2);
        assertTrue(i1.getNext().getNext() == i3);
        assertNull(i1.getNext().getNext().getNext());

        assertTrue(i3.getPrevious() == i2);
        assertTrue(i3.getPrevious().getPrevious() == i1);
        assertNull(i3.getPrevious().getPrevious().getPrevious());

        assertTrue(i1.isHeadNode());
        assertFalse(i1.isTailNode());
        assertFalse(i2.isHeadNode());
        assertFalse(i2.isTailNode());
        assertTrue(i3.isTailNode());
        assertFalse(i3.isHeadNode());

        i2.linkBefore(i5.linkBefore(i4));

        // Order should be 1,4,5,2,3

        assertTrue(i1.getNext() == i4);
        assertTrue(i1.getNext().getNext() == i5);
        assertTrue(i1.getNext().getNext().getNext() == i2);
        assertTrue(i1.getNext().getNext().getNext().getNext() == i3);
        assertNull(i1.getNext().getNext().getNext().getNext().getNext());

        assertTrue(i3.getPrevious() == i2);
        assertTrue(i3.getPrevious().getPrevious() == i5);
        assertTrue(i3.getPrevious().getPrevious().getPrevious() == i4);
        assertTrue(i3.getPrevious().getPrevious().getPrevious().getPrevious() == i1);
        assertNull(i3.getPrevious().getPrevious().getPrevious().getPrevious().getPrevious());

        assertTrue(i1.isHeadNode());
        assertFalse(i1.isTailNode());
        assertFalse(i4.isHeadNode());
        assertFalse(i4.isTailNode());
        assertFalse(i5.isHeadNode());
        assertFalse(i5.isTailNode());
        assertFalse(i2.isHeadNode());
        assertFalse(i2.isTailNode());
        assertTrue(i3.isTailNode());
        assertFalse(i3.isHeadNode());

    }

    public void testUnlink() {

        i1.linkAfter(i2.linkAfter(i3));
        i3.linkAfter(i4);
        i1.linkBefore(i5);
        i1.linkAfter(i6);

        // Order should be 5,1,6,2,3,4
        i4.unlink();
        i5.unlink();
        i6.unlink();

        // Order should be 1,2,3

        assertTrue(i1.getNext() == i2);
        assertTrue(i1.getNext().getNext() == i3);
        assertNull(i1.getNext().getNext().getNext());

        assertTrue(i3.getPrevious() == i2);
        assertTrue(i3.getPrevious().getPrevious() == i1);
        assertNull(i3.getPrevious().getPrevious().getPrevious());

        assertTrue(i1.isHeadNode());
        assertFalse(i1.isTailNode());
        assertFalse(i2.isHeadNode());
        assertFalse(i2.isTailNode());
        assertTrue(i3.isTailNode());
        assertFalse(i3.isHeadNode());
    }

}
