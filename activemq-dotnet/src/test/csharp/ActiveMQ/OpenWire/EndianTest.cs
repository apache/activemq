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
using ActiveMQ.OpenWire;
using NUnit.Framework;
using System;
using System.IO;

namespace ActiveMQ.OpenWire
{
    [TestFixture]
    public class EndianTest
    {
        
        [Test]
        public void TestLongEndian()
        {
            long value = 0x0102030405060708L;
            long newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            Assert.AreEqual(0x0807060504030201L, newValue);
            long actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestIntEndian()
        {
            int value = 0x12345678;
            int newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            Assert.AreEqual(0x78563412, newValue);
            int actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestCharEndian()
        {
            char value = 'J';
            char newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            char actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }

        [Test]
        public void TestShortEndian()
        {
            short value = 0x1234;
            short newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            Assert.AreEqual(0x3412, newValue);
            short actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestNegativeLongEndian()
        {
            long value = -0x0102030405060708L;
            long newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            long actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestNegativeIntEndian()
        {
            int value = -0x12345678;
            int newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            int actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestNegativeShortEndian()
        {
            short value = -0x1234;
            short newValue = EndianSupport.SwitchEndian(value);
            Console.WriteLine("New value: " + newValue);
            short actual = EndianSupport.SwitchEndian(newValue);
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestFloatDontNeedEndianSwitch()
        {
            float value = -1.223F;
            Console.WriteLine("value: " + value);
            
            // Convert to int so we can compare to Java version.
            MemoryStream ms = new MemoryStream(4);
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write(value);
            bw.Close();
            ms = new MemoryStream(ms.ToArray());
            BinaryReader br = new BinaryReader(ms);
                        
            // System.out.println(Integer.toString(Float.floatToIntBits(-1.223F), 16));
            Assert.AreEqual(-0x406374bc, br.ReadInt32());
            
        }
        
        [Test]
        public void TestDoublDontNeedEndianSwitch()
        {
            double value = -1.223D;
            Console.WriteLine("New value: " + value);
            
            // Convert to int so we can compare to Java version.
            MemoryStream ms = new MemoryStream(4);
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write(value);
            bw.Close();
            ms = new MemoryStream(ms.ToArray());
            BinaryReader br = new BinaryReader(ms);
            long longVersion = br.ReadInt64();
            
            // System.out.println(Long.toString(Double.doubleToLongBits(-1.223D), 16));
            Assert.AreEqual(-0x400c6e978d4fdf3b, longVersion);
        }
    }
}



