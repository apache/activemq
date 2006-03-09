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



namespace NMS
{
	[TestFixture]
    public class ConsumerTest : JMSTestSupport
    {
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
		
		protected override IConnection CreateConnection()
		{
			IConnection connection = base.CreateConnection();
			connection.ClientId = "test";
			return connection;
		}
		
		protected override IDestination CreateDestination()
		{
            return session.GetTopic(CreateDestinationName());
		}
		
		
        //[Ignore("Not fully implemented yet.")]
        [Test]
        public void testDurableConsumerSelectorChange()
        {
            
            IMessageProducer producer = session.CreateProducer(Destination);
            producer.Persistent = true;
            IMessageConsumer consumer = session.CreateDurableConsumer((ITopic)Destination, "test", "color='red'", false);
			
            // Send the messages
            ITextMessage message = session.CreateTextMessage("1st");
            message.Properties["color"] =  "red";
            producer.Send(message);
            
            IMessage m = consumer.Receive(receiveTimeout);
            Assert.IsNotNull(m);
            Assert.AreEqual("1st", ((ITextMessage)m).Text);
			
            // Change the subscription.
            consumer.Dispose();
            consumer = session.CreateDurableConsumer((ITopic)Destination, "test", "color='blue'", false);
            
            message = session.CreateTextMessage("2nd");
            message.Properties["color"] =  "red";
            producer.Send(message);
            message = session.CreateTextMessage("3rd");
            message.Properties["color"] =  "blue";
            producer.Send(message);
			
            // Selector should skip the 2nd message.
            m = consumer.Receive(1000);
            Assert.IsNotNull(m);
            Assert.AreEqual("3rd", ((ITextMessage)m).Text);
            
            Assert.IsNull(consumer.ReceiveNoWait());
        }
		
    }
}



