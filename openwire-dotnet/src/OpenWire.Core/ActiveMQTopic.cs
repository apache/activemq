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
		
		public String GetTopicName() 
		{
			return super.GetPhysicalName();
		}
		public override int GetDestinationType() 
		{
			return ACTIVEMQ_TOPIC;
		}


		public override ActiveMQDestination CreateDestination(String name) 
		{
			return new ActiveMQTopic(name);
		}

	}
}
