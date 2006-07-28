/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using NUnit.Framework;
using System;
using System.IO;

namespace ActiveMQ.OpenWire {
        [ TestFixture ]
        public class BooleanStreamTest
        {
                protected int endOfStreamMarker = 0x12345678;
                int numberOfBytes = 8 * 200;

                public delegate bool GetBooleanValueDelegate(int index, int count);

                [ Test ]
                public void TestBooleanMarshallingUsingAllTrue()
                {
                        DoTestBooleanStream(numberOfBytes, new GetBooleanValueDelegate(GetBooleanValueAllTrue));
                }
                public bool GetBooleanValueAllTrue(int index, int count)
                {
                        return true;
                }

                [ Test ]
                public void TestBooleanMarshallingUsingAllFalse()
                {
                        DoTestBooleanStream(numberOfBytes, new GetBooleanValueDelegate(GetBooleanValueAllFalse));
                }
                public bool GetBooleanValueAllFalse(int index, int count)
                {
                        return false;
                }

                [ Test ]
                public void TestBooleanMarshallingUsingAlternateTrueFalse()
                {
                        DoTestBooleanStream(
                                numberOfBytes, new GetBooleanValueDelegate(GetBooleanValueAlternateTrueFalse));
                }
                public bool GetBooleanValueAlternateTrueFalse(int index, int count)
                {
                        return (index & 1) == 0;
                }

                [ Test ]
                public void TestBooleanMarshallingUsingAlternateFalseTrue()
                {
                        DoTestBooleanStream(
                                numberOfBytes, new GetBooleanValueDelegate(GetBooleanValueAlternateFalseTrue));
                }
                public bool GetBooleanValueAlternateFalseTrue(int index, int count)
                {
                        return (index & 1) != 0;
                }

                protected void DoTestBooleanStream(int numberOfBytes, GetBooleanValueDelegate valueDelegate)
                {
                        for (int i = 1017; i < numberOfBytes; i++)
                        {
                                AssertMarshalBooleans(i, valueDelegate);
                        }
                }

                protected void AssertMarshalBooleans(int count, GetBooleanValueDelegate valueDelegate)
                {
                        BooleanStream bs = new BooleanStream();
                        for (int i = 0; i < count; i++)
                        {
                                bs.WriteBoolean(valueDelegate(i, count));
                        }
                        MemoryStream buffer = new MemoryStream();
                        BinaryWriter ds = new OpenWireBinaryWriter(buffer);
                        bs.Marshal(ds);
                        ds.Write(endOfStreamMarker);

                        // now lets read from the stream

                        MemoryStream ins = new MemoryStream(buffer.ToArray());
                        BinaryReader dis = new OpenWireBinaryReader(ins);
                        bs = new BooleanStream();
                        bs.Unmarshal(dis);

                        for (int i = 0; i < count; i++)
                        {
                                bool expected = valueDelegate(i, count);

                                try
                                {
                                        bool actual = bs.ReadBoolean();
                                        Assert.AreEqual(expected, actual);
                                }
                                catch (Exception e)
                                {
                                        Assert.Fail(
                                                "Failed to parse bool: " + i + " out of: " + count + " due to: " + e);
                                }
                        }
                        int marker = dis.ReadInt32();
                        Assert.AreEqual(
                                endOfStreamMarker, marker, "did not match: " + endOfStreamMarker + " and " + marker);

                        // lets try read and we should get an exception
                        try
                        {
                                dis.ReadByte();
                                Assert.Fail("Should have reached the end of the stream");
                        }
                        catch (IOException)
                        {
                        }
                }
        }
}
