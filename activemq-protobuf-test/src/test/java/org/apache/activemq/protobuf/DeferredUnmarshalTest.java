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

import junit.framework.TestCase;

import org.apache.activemq.protobuf.DeferredUnmarshal.Bar;
import org.apache.activemq.protobuf.DeferredUnmarshal.Foo;

public class DeferredUnmarshalTest extends TestCase {

    public void testDeferredDecoding() throws InvalidProtocolBufferException {

        Foo foo = new Foo();
        foo.setField1(5);
        foo.setField2(20);

        Bar bar = new Bar();

        // There is no decoding pending so its' considered decoded.
        assertTrue(bar.isDecoded());

        bar.setField1(25);
        bar.setField2(220);
        bar.setField3(foo);

        // The message should not be encoded yet.
        assertFalse(bar.isEncoded());

        // The message should be encoded now..
        byte[] encodedForm = bar.toUnframedByteArray();
        assertTrue(bar.isEncoded());

        // Repeated encoding operations should just give back the same byte[]
        assertTrue(encodedForm == bar.toUnframedByteArray());

        // Decoding does not occur until a field is accessed. The new message should
        // still be considered encoded.
        Bar bar2 = Bar.parseUnframed(encodedForm);
        assertTrue(bar2.isEncoded());
        assertFalse(bar2.isDecoded());

        // This should now decode the message.
        assertEquals(25, bar2.getField1());
        assertTrue(bar2.isDecoded());

        // Since bar2 still has not been modified it should still spit out the same
        // byte[]
        assertTrue(encodedForm == bar2.toUnframedByteArray());

        // Nested messages should remain un-decoded.
        assertFalse(bar2.getField3().isDecoded());

        // Changing a field should remove the encoding.
        bar2.setField1(35);
        assertFalse(bar2.isEncoded());
        assertTrue(bar2.isDecoded());

    }

}
