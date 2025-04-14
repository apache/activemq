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
package org.apache.activemq.protobuf;

import org.apache.activemq.protobuf.DeferredUnmarshal.Bar;
import org.apache.activemq.protobuf.DeferredUnmarshal.Foo;

import junit.framework.TestCase;

public class EqualsTest extends TestCase {

    public void testDeferredUnmarshal() {

        Bar bar1 = createBar();
        Bar bar2 = createBar();

        // They should have the same hash and equal the same value.
        assertTrue(bar1.hashCode() == bar2.hashCode());
        assertTrue(bar1.equals(bar2));

        // Change bar2 a little.

        bar2.setField2(35);

        assertFalse(bar1.hashCode() == bar2.hashCode());
        assertFalse(bar1.equals(bar2));

    }

    private Bar createBar() {
        Bar bar;
        Foo foo = new Foo();
        foo.setField1(5);
        foo.setField2(20);

        bar = new Bar();
        bar.setField1(25);
        bar.setField2(220);
        bar.setField3(foo);
        return bar;
    }

}
