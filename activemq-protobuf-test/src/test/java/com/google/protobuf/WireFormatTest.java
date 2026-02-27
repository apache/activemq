// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.
// http://code.google.com/p/protobuf/
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.protobuf;

import junit.framework.TestCase;

import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.CodedInputStream;

import protobuf_unittest.UnittestProto.TestAllTypes;

/**
 * Tests related to parsing and serialization.
 *
 * @author kenton@google.com (Kenton Varda)
 */
public class WireFormatTest extends TestCase {
  public void testSerialization() throws Exception {
    TestAllTypes message = TestUtil.getAllSet();

    byte[] rawBytes = message.toUnframedByteArray();
    assertEquals(rawBytes.length, message.serializedSizeUnframed());

    TestAllTypes message2 = TestAllTypes.parseUnframed(rawBytes);

    TestUtil.assertAllFieldsSet(message2);
  }

  private void assertFieldsInOrder(Buffer data) throws Exception {
    try(CodedInputStream input = new CodedInputStream(data)) {
      int previousTag = 0;

      while (true) {
        int tag = input.readTag();
        if (tag == 0) {
          break;
        }

        assertTrue(tag > previousTag);
        input.skipField(tag);
      }
    }
  }

}

