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

import protobuf_unittest.UnittestProto.TestAllTypes;
import protobuf_unittest.UnittestProto.TestRequired;
import protobuf_unittest.UnittestProto.TestRequiredForeign;
import protobuf_unittest.UnittestProto.ForeignMessage;

import junit.framework.TestCase;

/**
 * Misc. unit tests for message operations that apply to both generated
 * and dynamic messages.
 *
 * @author kenton@google.com Kenton Varda
 */
public class MessageTest extends TestCase {
  // =================================================================
  // Message-merging tests.

  static final TestAllTypes MERGE_SOURCE =
    new TestAllTypes()
      .setOptionalInt32(1)
      .setOptionalString("foo")
      .setOptionalForeignMessage(new ForeignMessage())
      .addRepeatedString("bar")
      ;

  static final TestAllTypes MERGE_DEST =
    new TestAllTypes()
      .setOptionalInt64(2)
      .setOptionalString("baz")
      .setOptionalForeignMessage(new ForeignMessage().setC(3))
      .addRepeatedString("qux")
      ;

  static final String MERGE_RESULT_TEXT =
      "optional_int32: 1\n" +
      "optional_int64: 2\n" +
      "optional_string: foo\n" +
      "optional_foreign_message {\n" +
      "  c: 3\n" +
      "}\n" +
      "repeated_string[0]: qux\n" +
      "repeated_string[1]: bar\n";

  public void testMergeFrom() throws Exception {
    TestAllTypes result =
      new TestAllTypes().mergeFrom(MERGE_DEST)
        .mergeFrom(MERGE_SOURCE);

    assertEquals(MERGE_RESULT_TEXT, result.toString());
  }


  // =================================================================
  // Required-field-related tests.

  private static final TestRequired TEST_REQUIRED_UNINITIALIZED =
    new TestRequired();
  private static final TestRequired TEST_REQUIRED_INITIALIZED =
    new TestRequired().setA(1).setB(2).setC(3);

  public void testRequired() throws Exception {
    TestRequired builder = new TestRequired();

    assertFalse(builder.isInitialized());
    builder.setA(1);
    assertFalse(builder.isInitialized());
    builder.setB(1);
    assertFalse(builder.isInitialized());
    builder.setC(1);
    assertTrue(builder.isInitialized());
  }

  public void testRequiredForeign() throws Exception {
    TestRequiredForeign builder = new TestRequiredForeign();

    assertTrue(builder.isInitialized());

    builder.setOptionalMessage(TEST_REQUIRED_UNINITIALIZED);
    assertFalse(builder.isInitialized());

    builder.setOptionalMessage(TEST_REQUIRED_INITIALIZED);
    assertTrue(builder.isInitialized());

    builder.addRepeatedMessage(TEST_REQUIRED_UNINITIALIZED);
    assertFalse(builder.isInitialized());

    builder.setRepeatedMessage(0, TEST_REQUIRED_INITIALIZED);
    assertTrue(builder.isInitialized());
  }


  public void testIsInitialized() throws Exception {
      assertFalse(new TestRequired().isInitialized());
  }

}
