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
//using ActiveMQ;
using NMS;
using NUnit.Framework;
using System;



namespace NMS
{
	[ TestFixture ]
    public class JMSPropertyTest : JMSTestSupport
    {
        // standard JMS properties
        string expectedText = "Hey this works!";
        string correlationID = "abc";
        ITemporaryQueue replyTo;
        bool persistent = true;
        byte priority = 5;
        String type = "FooType";
        String groupID = "MyGroup";
        int groupSeq = 1;
        
        // custom properties
        string customText = "Cheese";
        bool custom1 = true;
        byte custom2 = 12;
        short custom3 = 0x1234;
        int custom4 = 0x12345678;
        long custom5 = 0x1234567812345678;
        char custom6 = 'J';
        
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
            ITextMessage message = session.CreateTextMessage(expectedText);
            replyTo = session.CreateTemporaryQueue();
            
            // lets set the headers
            message.NMSCorrelationID = correlationID;
            message.NMSReplyTo = replyTo;
            message.NMSPersistent = persistent;
            message.NMSPriority = priority;
            message.NMSType = type;
            message.Properties["JMSXGroupID"] = groupID;
            message.Properties["JMSXGroupSeq"] = groupSeq;
            
            // lets set the custom headers
            message.Properties["customText"] = customText;
            message.Properties["custom1"] = custom1;
            message.Properties["custom2"] = custom2;
            message.Properties["custom3"] = custom3;
            message.Properties["custom4"] = custom4;
            message.Properties["custom5"] = custom5;
            message.Properties["custom6"] = custom6;
            
            return message;
        }
        
        protected override void AssertValidMessage(IMessage message)
        {
            Assert.IsTrue(message is ITextMessage, "Did not receive a ITextMessage!");
            
            Console.WriteLine("Received Message: " + message);
            
            ITextMessage textMessage = (ITextMessage) message;
            String text = textMessage.Text;
            Assert.AreEqual(expectedText, text, "the message text");
            
            // compare standard JMS headers
            Assert.AreEqual(correlationID, message.NMSCorrelationID, "JMSCorrelationID");
            Assert.AreEqual(replyTo, message.NMSReplyTo, "JMSReplyTo");
            Assert.AreEqual(persistent, message.NMSPersistent, "JMSPersistent");
            Assert.AreEqual(priority, message.NMSPriority, "JMSPriority");
            Assert.AreEqual(type, message.NMSType, "JMSType");
            Assert.AreEqual(groupID, message.Properties["JMSXGroupID"], "JMSXGroupID");
            Assert.AreEqual(groupSeq, message.Properties["JMSXGroupSeq"], "JMSXGroupSeq");
            
            // compare custom headers
            Assert.AreEqual(customText, message.Properties["customText"], "customText");
            Assert.AreEqual(custom1, message.Properties["custom1"], "custom1");
            Assert.AreEqual(custom2, message.Properties["custom2"], "custom2");
            Assert.AreEqual(custom3, message.Properties["custom3"], "custom3");
            Assert.AreEqual(custom4, message.Properties["custom4"], "custom4");
            // TODO
            Assert.AreEqual(custom5, message.Properties["custom5"], "custom5");
            Assert.AreEqual(custom6, message.Properties["custom6"], "custom6");
            
            Assert.AreEqual(custom1, message.Properties.GetBool("custom1"), "custom1");
            Assert.AreEqual(custom2, message.Properties.GetByte("custom2"), "custom2");
            Assert.AreEqual(custom3, message.Properties.GetShort("custom3"), "custom3");
            Assert.AreEqual(custom4, message.Properties.GetInt("custom4"), "custom4");
            Assert.AreEqual(custom5, message.Properties.GetLong("custom5"), "custom5");
            Assert.AreEqual(custom6, message.Properties.GetChar("custom6"), "custom6");
            
            // lets now look at some standard JMS headers
            Console.WriteLine("JMSExpiration: " + message.NMSExpiration);
            Console.WriteLine("JMSMessageId: " + message.NMSMessageId);
            Console.WriteLine("JMSRedelivered: " + message.NMSRedelivered);
            Console.WriteLine("JMSTimestamp: " + message.NMSTimestamp);
            Console.WriteLine("JMSXDeliveryCount: " + message.Properties["JMSXDeliveryCount"]);
            Console.WriteLine("JMSXProducerTXID: " + message.Properties["JMSXProducerTXID"]);
        }
    }
}



