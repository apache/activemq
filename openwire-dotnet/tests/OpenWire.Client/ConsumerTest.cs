using System;
using System.IO;

using NUnit.Framework;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    [TestFixture]
    public class ConsumerTest : TestSupport
    {
		IConnectionFactory factory;
		IConnection connection;
		IDestination destination;

		[SetUp]
		protected void SetUp() 
		{
			factory = new ConnectionFactory("localhost", 61616);
			connection = factory.CreateConnection();
		}

		[TearDown]
		protected void TearDown() 
		{
			connection.Dispose();
		}

        [Test]
		[Ignore("Not fully implemented yet.")]
		public void testDurableConsumerSelectorChange()  
		{

			// Receive a message with the JMS API
			connection.ClientId="test";
			connection.Start();
			
			ISession session = connection.CreateSession(false, AcknowledgementMode.AutoAcknowledge);
			destination = session.GetTopic("foo");
			IMessageProducer producer = session.CreateProducer(destination);
			producer.Persistent = true;
			IMessageConsumer consumer = session.CreateDurableConsumer((ITopic)destination, "test", "color='red'", false);

			// Send the messages
			ITextMessage message = session.CreateTextMessage("1st");
			//message.SetStringProperty("color", "red");
			producer.Send(message);
	        
			IMessage m = consumer.Receive(1000);
			Assert.IsNotNull(m);
			Assert.AreEqual("1st", ((ITextMessage)m).Text );

			// Change the subscription.
			consumer.Dispose();
			consumer = session.CreateDurableConsumer((ITopic)destination, "test", "color='blue'", false);
	        
			message = session.CreateTextMessage("2nd");
			// message.setStringProperty("color", "red");
			producer.Send(message);
			message = session.CreateTextMessage("3rd");
			 // message.setStringProperty("color", "blue");
			producer.Send(message);

			// Selector should skip the 2nd message.
			m = consumer.Receive(1000);
			Assert.IsNotNull(m);
			Assert.AreEqual("3rd", ((ITextMessage)m).Text);
	        
			Assert.IsNull(consumer.ReceiveNoWait());
		}

    }
}

