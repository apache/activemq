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
using ActiveMQ.OpenWire;

namespace ActiveMQ.OpenWire
{
    [TestFixture]
    public class PrimitiveMapTest
    {
		
		bool a = true;
        byte b = 123;
        char c = 'c';
        short d = 0x1234;
        int e = 0x12345678;
        long f = 0x1234567812345678;
        string g = "Hello World!";
		bool h = false;
        byte i = 0xFF;
        short j = -0x1234;
        int k = -0x12345678;
        long l = -0x1234567812345678;
		
        [Test]
        public void TestNotMarshalled()
        {
			PrimitiveMap map = CreatePrimitiveMap();
			AssertPrimitiveMap(map);
        }
		
        [Test]
        public void TestMarshalled()
        {
			PrimitiveMap map = CreatePrimitiveMap();
			Console.WriteLine("data: "+map);
			byte[] data = map.Marshal();
			map = PrimitiveMap.Unmarshal(data);
			Console.WriteLine("data: "+map);
			AssertPrimitiveMap(map);
        }
		
		protected PrimitiveMap CreatePrimitiveMap()
        {
            PrimitiveMap map = new PrimitiveMap();
            
            map["a"] = a;
            map["b"] = b;
            map["c"] = c;
            map["d"] = d;
            map["e"] = e;
            map["f"] = f;
            map["g"] = g;
            map["h"] = h;
            map["i"] = i;
            map["j"] = j;
            map["k"] = k;
            map["l"] = l;
            
            return map;
        }
		
		protected void AssertPrimitiveMap(PrimitiveMap map) {
			// use generic API to access entries
			Assert.AreEqual(a, map["a"], "generic map entry: a");
			Assert.AreEqual(b, map["b"], "generic map entry: b");
			Assert.AreEqual(c, map["c"], "generic map entry: c");
			Assert.AreEqual(d, map["d"], "generic map entry: d");
			Assert.AreEqual(e, map["e"], "generic map entry: e");
			Assert.AreEqual(f, map["f"], "generic map entry: f");
			Assert.AreEqual(g, map["g"], "generic map entry: g");
			Assert.AreEqual(h, map["h"], "generic map entry: h");
			Assert.AreEqual(i, map["i"], "generic map entry: i");
			Assert.AreEqual(j, map["j"], "generic map entry: j");
			Assert.AreEqual(k, map["k"], "generic map entry: k");
			Assert.AreEqual(l, map["l"], "generic map entry: l");
			
			// use type safe APIs
			Assert.AreEqual(a, map.GetBool("a"), "map entry: a");
			Assert.AreEqual(b, map.GetByte("b"), "map entry: b");
			Assert.AreEqual(c, map.GetChar("c"), "map entry: c");
			Assert.AreEqual(d, map.GetShort("d"), "map entry: d");
			Assert.AreEqual(e, map.GetInt("e"), "map entry: e");
			Assert.AreEqual(f, map.GetLong("f"), "map entry: f");
			Assert.AreEqual(g, map.GetString("g"), "map entry: g");
			Assert.AreEqual(h, map.GetBool("h"), "map entry: h");
			Assert.AreEqual(i, map.GetByte("i"), "map entry: i");
			Assert.AreEqual(j, map.GetShort("j"), "map entry: j");
			Assert.AreEqual(k, map.GetInt("k"), "map entry: k");
			Assert.AreEqual(l, map.GetLong("l"), "map entry: l");
		}
		
		
	}
}

