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

import java.util.Arrays;

import junit.framework.TestCase;
import protobuf_unittest.EnumWithNoOuter;
import protobuf_unittest.MessageWithNoOuter;
import protobuf_unittest.UnittestProto.ForeignEnum;
import protobuf_unittest.UnittestProto.ForeignMessage;
import protobuf_unittest.UnittestProto.TestAllTypes;
import protobuf_unittest.UnittestProto.TestExtremeDefaultValues;

/**
 * Unit test for generated messages and generated code.  See also
 * {@link MessageTest}, which tests some generated message functionality.
 *
 * @author kenton@google.com Kenton Varda
 */
public class GeneratedMessageTest extends TestCase {


  public void testAccessors() throws Exception {
    TestAllTypes builder = new TestAllTypes();
    TestUtil.setAllFields(builder);
    TestAllTypes message = builder;
    TestUtil.assertAllFieldsSet(message);
  }

  public void testRepeatedSetters() throws Exception {
    TestAllTypes builder = new TestAllTypes();
    TestUtil.setAllFields(builder);
    TestUtil.modifyRepeatedFields(builder);
    TestAllTypes message = builder;
    TestUtil.assertRepeatedFieldsModified(message);
  }

  public void testRepeatedAppend() throws Exception {
    TestAllTypes builder = new TestAllTypes();

    builder.addAllRepeatedInt32(Arrays.asList(1, 2, 3, 4));
    builder.addAllRepeatedForeignEnum(Arrays.asList(ForeignEnum.FOREIGN_BAZ));

    ForeignMessage foreignMessage =  new ForeignMessage().setC(12);
    builder.addAllRepeatedForeignMessage(Arrays.asList(foreignMessage));

    TestAllTypes message = builder;
    assertEquals(message.getRepeatedInt32List(), Arrays.asList(1, 2, 3, 4));
    assertEquals(message.getRepeatedForeignEnumList(),
        Arrays.asList(ForeignEnum.FOREIGN_BAZ));
    assertEquals(1, message.getRepeatedForeignMessageCount());
    assertEquals(12, message.getRepeatedForeignMessage(0).getC());
  }

  public void testSettingForeignMessageUsingBuilder() throws Exception {
    TestAllTypes message = new TestAllTypes()
        // Pass builder for foreign message instance.
        .setOptionalForeignMessage(new ForeignMessage().setC(123))
        ;
    TestAllTypes expectedMessage = new TestAllTypes()
        // Create expected version passing foreign message instance explicitly.
        .setOptionalForeignMessage(new ForeignMessage().setC(123))
        ;
    // TODO(ngd): Upgrade to using real #equals method once implemented
    assertEquals(expectedMessage.toString(), message.toString());
  }

  public void testSettingRepeatedForeignMessageUsingBuilder() throws Exception {
    TestAllTypes message = new TestAllTypes()
        // Pass builder for foreign message instance.
        .addRepeatedForeignMessage(new ForeignMessage().setC(456))
        ;
    TestAllTypes expectedMessage = new TestAllTypes()
        // Create expected version passing foreign message instance explicitly.
        .addRepeatedForeignMessage(
            new ForeignMessage().setC(456))
        ;
    assertEquals(expectedMessage.toString(), message.toString());
  }

  public void testDefaults() throws Exception {
    TestUtil.assertClear(new TestAllTypes());

    assertEquals("\u1234", new TestExtremeDefaultValues().getUtf8String());
  }

  // =================================================================
  // multiple_files_test

  public void testMultipleFilesOption() throws Exception {
    // We mostly just want to check that things compile.
    MessageWithNoOuter message =
      new MessageWithNoOuter()
        .setNested(new MessageWithNoOuter.NestedMessage().setI(1))
        .addForeign(new TestAllTypes().setOptionalInt32(1))
        .setNestedEnum(MessageWithNoOuter.NestedEnum.BAZ)
        .setForeignEnum(EnumWithNoOuter.BAR)
        ;
    
    byte[] data = message.toUnframedByteArray();
    MessageWithNoOuter newMessage = MessageWithNoOuter.parseUnframed(data);
    assertEquals(message.toString(), newMessage.toString());
  }
}
