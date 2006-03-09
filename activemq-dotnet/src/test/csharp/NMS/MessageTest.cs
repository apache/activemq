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
using NMS;
using NUnit.Framework;
using System;


namespace NMS
{
	[ TestFixture ]
	public class MessageTest : JMSTestSupport
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
        
		[SetUp]
        override public void SetUp()
        {
			base.SetUp();
        }
		
        [TearDown]
        override public void TearDown()
        {
			base.TearDown();
        }
		
        [ Test ]
        public override void SendAndSyncReceive()
        {
            base.SendAndSyncReceive();
        }
		
        protected override IMessage CreateMessage()
        {
            IMessage message = session.CreateMessage();
            
            message.Properties["a"] = a;
            message.Properties["b"] = b;
            message.Properties["c"] = c;
            message.Properties["d"] = d;
            message.Properties["e"] = e;
            message.Properties["f"] = f;
            message.Properties["g"] = g;
            message.Properties["h"] = h;
            message.Properties["i"] = i;
            message.Properties["j"] = j;
            message.Properties["k"] = k;
            message.Properties["l"] = l;
            
            return message;
        }
        
        protected override void AssertValidMessage(IMessage message)
        {
            Console.WriteLine("Received message: " + message);
            Console.WriteLine("Received Count: " + message.Properties.Count);
			
            Assert.AreEqual(ToHex(f), ToHex(message.Properties.GetLong("f")), "map entry: f as hex");
            
            // use generic API to access entries
            Assert.AreEqual(a, message.Properties["a"], "generic map entry: a");
            Assert.AreEqual(b, message.Properties["b"], "generic map entry: b");
            Assert.AreEqual(c, message.Properties["c"], "generic map entry: c");
            Assert.AreEqual(d, message.Properties["d"], "generic map entry: d");
            Assert.AreEqual(e, message.Properties["e"], "generic map entry: e");
            Assert.AreEqual(f, message.Properties["f"], "generic map entry: f");
            Assert.AreEqual(g, message.Properties["g"], "generic map entry: g");
            Assert.AreEqual(h, message.Properties["h"], "generic map entry: h");
            Assert.AreEqual(i, message.Properties["i"], "generic map entry: i");
            Assert.AreEqual(j, message.Properties["j"], "generic map entry: j");
            Assert.AreEqual(k, message.Properties["k"], "generic map entry: k");
            Assert.AreEqual(l, message.Properties["l"], "generic map entry: l");
            
            // use type safe APIs
            Assert.AreEqual(a, message.Properties.GetBool("a"), "map entry: a");
            Assert.AreEqual(b, message.Properties.GetByte("b"), "map entry: b");
            Assert.AreEqual(c, message.Properties.GetChar("c"), "map entry: c");
            Assert.AreEqual(d, message.Properties.GetShort("d"), "map entry: d");
            Assert.AreEqual(e, message.Properties.GetInt("e"), "map entry: e");
            Assert.AreEqual(f, message.Properties.GetLong("f"), "map entry: f");
            Assert.AreEqual(g, message.Properties.GetString("g"), "map entry: g");
            Assert.AreEqual(h, message.Properties.GetBool("h"), "map entry: h");
            Assert.AreEqual(i, message.Properties.GetByte("i"), "map entry: i");
            Assert.AreEqual(j, message.Properties.GetShort("j"), "map entry: j");
            Assert.AreEqual(k, message.Properties.GetInt("k"), "map entry: k");
            Assert.AreEqual(l, message.Properties.GetLong("l"), "map entry: l");
            
        }
        
        protected string ToHex(long value)
        {
            return String.Format("{0:x}", value);
        }
		
	}
}

