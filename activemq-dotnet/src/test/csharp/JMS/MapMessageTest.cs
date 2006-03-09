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


namespace JMS
{
	[ TestFixture ]
    public class MapMessageTest : JMSTestSupport
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
            IMapMessage message = session.CreateMapMessage();
            
            message.Body["a"] = a;
            message.Body["b"] = b;
            message.Body["c"] = c;
            message.Body["d"] = d;
            message.Body["e"] = e;
            message.Body["f"] = f;
            message.Body["g"] = g;
            message.Body["h"] = h;
            message.Body["i"] = i;
            message.Body["j"] = j;
            message.Body["k"] = k;
            message.Body["l"] = l;
            
            return message;
        }
        
        protected override void AssertValidMessage(IMessage message)
        {
            Assert.IsTrue(message is IMapMessage, "Did not receive a MapMessage!");
            IMapMessage mapMessage = (IMapMessage) message;
            
            Console.WriteLine("Received MapMessage: " + message);
            Console.WriteLine("Received Count: " + mapMessage.Body.Count);
			
            Assert.AreEqual(ToHex(f), ToHex(mapMessage.Body.GetLong("f")), "map entry: f as hex");
            
            // use generic API to access entries
            Assert.AreEqual(a, mapMessage.Body["a"], "generic map entry: a");
            Assert.AreEqual(b, mapMessage.Body["b"], "generic map entry: b");
            Assert.AreEqual(c, mapMessage.Body["c"], "generic map entry: c");
            Assert.AreEqual(d, mapMessage.Body["d"], "generic map entry: d");
            Assert.AreEqual(e, mapMessage.Body["e"], "generic map entry: e");
            Assert.AreEqual(f, mapMessage.Body["f"], "generic map entry: f");
            Assert.AreEqual(g, mapMessage.Body["g"], "generic map entry: g");
            Assert.AreEqual(h, mapMessage.Body["h"], "generic map entry: h");
            Assert.AreEqual(i, mapMessage.Body["i"], "generic map entry: i");
            Assert.AreEqual(j, mapMessage.Body["j"], "generic map entry: j");
            Assert.AreEqual(k, mapMessage.Body["k"], "generic map entry: k");
            Assert.AreEqual(l, mapMessage.Body["l"], "generic map entry: l");
            
            // use type safe APIs
            Assert.AreEqual(a, mapMessage.Body.GetBool("a"), "map entry: a");
            Assert.AreEqual(b, mapMessage.Body.GetByte("b"), "map entry: b");
            Assert.AreEqual(c, mapMessage.Body.GetChar("c"), "map entry: c");
            Assert.AreEqual(d, mapMessage.Body.GetShort("d"), "map entry: d");
            Assert.AreEqual(e, mapMessage.Body.GetInt("e"), "map entry: e");
            Assert.AreEqual(f, mapMessage.Body.GetLong("f"), "map entry: f");
            Assert.AreEqual(g, mapMessage.Body.GetString("g"), "map entry: g");
            Assert.AreEqual(h, mapMessage.Body.GetBool("h"), "map entry: h");
            Assert.AreEqual(i, mapMessage.Body.GetByte("i"), "map entry: i");
            Assert.AreEqual(j, mapMessage.Body.GetShort("j"), "map entry: j");
            Assert.AreEqual(k, mapMessage.Body.GetInt("k"), "map entry: k");
            Assert.AreEqual(l, mapMessage.Body.GetLong("l"), "map entry: l");
            			
        }
        
        protected string ToHex(long value)
        {
            return String.Format("{0:x}", value);
        }
    }
}

