using System;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for Topic.
	/// </summary>
	public class Topic : Destination 
	{
		public Topic(): base()	{}
		public Topic(String name):base(name){}
		
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
