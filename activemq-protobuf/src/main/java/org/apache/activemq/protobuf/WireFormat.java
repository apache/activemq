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

package org.apache.activemq.protobuf;

/**
 * This class is used internally by the Protocol Buffer library and generated
 * message implementations. It is public only because those generated messages
 * do not reside in the {@code protocol2} package. Others should not use this
 * class directly.
 * 
 * This class contains constants and helper functions useful for dealing with
 * the Protocol Buffer wire format.
 * 
 * @author kenton@google.com Kenton Varda
 */
public final class WireFormat {
    // Do not allow instantiation.
    private WireFormat() {
    }

    public static final int WIRETYPE_VARINT = 0;
    public static final int WIRETYPE_FIXED64 = 1;
    public static final int WIRETYPE_LENGTH_DELIMITED = 2;
    public static final int WIRETYPE_START_GROUP = 3;
    public static final int WIRETYPE_END_GROUP = 4;
    public static final int WIRETYPE_FIXED32 = 5;

    public static final int TAG_TYPE_BITS = 3;
    public static final int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;

    /** Given a tag value, determines the wire type (the lower 3 bits). */
    public static int getTagWireType(int tag) {
        return tag & TAG_TYPE_MASK;
    }

    /** Given a tag value, determines the field number (the upper 29 bits). */
    public static int getTagFieldNumber(int tag) {
        return tag >>> TAG_TYPE_BITS;
    }

    /** Makes a tag value given a field number and wire type. */
    public static int makeTag(int fieldNumber, int wireType) {
        return (fieldNumber << TAG_TYPE_BITS) | wireType;
    }

    // Field numbers for feilds in MessageSet wire format.
    public static final int MESSAGE_SET_ITEM = 1;
    public static final int MESSAGE_SET_TYPE_ID = 2;
    public static final int MESSAGE_SET_MESSAGE = 3;

    // Tag numbers.
    public static final int MESSAGE_SET_ITEM_TAG = makeTag(MESSAGE_SET_ITEM, WIRETYPE_START_GROUP);
    public static final int MESSAGE_SET_ITEM_END_TAG = makeTag(MESSAGE_SET_ITEM, WIRETYPE_END_GROUP);
    public static final int MESSAGE_SET_TYPE_ID_TAG = makeTag(MESSAGE_SET_TYPE_ID, WIRETYPE_VARINT);
    public static final int MESSAGE_SET_MESSAGE_TAG = makeTag(MESSAGE_SET_MESSAGE, WIRETYPE_LENGTH_DELIMITED);
}
