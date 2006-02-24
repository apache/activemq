/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
using OpenWire.Client.Core;
using System;

namespace openwire_dotnet
{
    [TestFixture]
    public class EndianTest
    {
        
        [Test]
        public void TestLongEndian()
        {
            long value = 0x0102030405060708L;
            
            long newValue = DataStreamMarshaller.SwitchEndian(value);
            
            Console.WriteLine("New value: " + newValue);
            
            Assert.AreEqual(0x0807060504030201L, newValue);
            
            long actual = DataStreamMarshaller.SwitchEndian(newValue);
            
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestIntEndian()
        {
            int value = 0x12345678;
            
            int newValue = DataStreamMarshaller.SwitchEndian(value);
            
            Console.WriteLine("New value: " + newValue);
            
            Assert.AreEqual(0x78563412, newValue);
            
            int actual = DataStreamMarshaller.SwitchEndian(newValue);
            
            Assert.AreEqual(value, actual);
        }
        [Test]
        public void TestShortEndian()
        {
            short value = 0x1234;
            
            short newValue = DataStreamMarshaller.SwitchEndian(value);
            
            Console.WriteLine("New value: " + newValue);
            
            Assert.AreEqual(0x3412, newValue);
            
            short actual = DataStreamMarshaller.SwitchEndian(newValue);
            
            Assert.AreEqual(value, actual);
        }
    }
}


