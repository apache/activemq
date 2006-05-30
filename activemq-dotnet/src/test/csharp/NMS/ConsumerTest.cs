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
	[TestFixture]
    public class ConsumerTest : JMSTestSupport
    {
		
		public bool persistent;
		public int prefetch;
		public bool durableConsumer;
		
		[SetUp]
        override public void SetUp()
        {
			clientId = "test";
			base.SetUp();
        }
		
        [TearDown]
        override public void TearDown()
        {
			base.TearDown();
        }
		
		
        [Test]
        public void TestDurableConsumerSelectorChangePersistent()
        {
			this.destinationType = DestinationType.Topic;
			this.persistent = true;
			doTestDurableConsumerSelectorChange();
		}
		
        [Test]
        public void TestDurableConsumerSelectorChangeNonPersistent()
        {
			this.destinationType = DestinationType.Topic;
			this.persistent = true;
			doTestDurableConsumerSelectorChange();
		}
		
        public void doTestDurableConsumerSelectorChange()
        {
            
            IMessageProducer producer = Session.CreateProducer(Destination);
			producer.Persistent = persistent;
            IMessageConsumer consumer = Session.CreateDurableConsumer((ITopic)Destination, "test", "color='red'", false);
			
            // Send the messages
            ITextMessage message = Session.CreateTextMessage("1st");
            message.Properties["color"] =  "red";
            producer.Send(message);
            
            IMessage m = consumer.Receive(TimeSpan.FromMilliseconds(receiveTimeout));
            Assert.IsNotNull(m);
            Assert.AreEqual("1st", ((ITextMessage)m).Text);
			
            // Change the subscription.
            consumer.Dispose();
            consumer = Session.CreateDurableConsumer((ITopic)Destination, "test", "color='blue'", false);
            
            message = Session.CreateTextMessage("2nd");
            message.Properties["color"] =  "red";
            producer.Send(message);
            message = Session.CreateTextMessage("3rd");
            message.Properties["color"] =  "blue";
            producer.Send(message);
			
            // Selector should skip the 2nd message.
            m = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(m);
            Assert.AreEqual("3rd", ((ITextMessage)m).Text);
            
            Assert.IsNull(consumer.ReceiveNoWait());
        }
				
    }
}



