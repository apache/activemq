using System;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for ActiveMQTopic.
	/// </summary>
	public class ActiveMQTopic : ActiveMQDestination 
	{
		public ActiveMQTopic(): base()	{}
		public ActiveMQTopic(String name):base(name){}
		public String getTopicName() 
		{
			return super.getPhysicalName();
		}
		public override int getDestinationType() 
		{
			return ACTIVEMQ_TOPIC;
		}


		public override ActiveMQDestination createDestination(String name) 
		{
			return new ActiveMQTopic(name);
		}

	}
}
