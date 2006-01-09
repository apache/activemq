using System;
using OpenWire.Core;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for ActiveMQTempTopic.
	/// </summary>
	public class ActiveMQTempTopic : ActiveMQDestination, TemporaryTopic
	{
		public const byte ID_ActiveMQTempTopic = 103;
		
		public ActiveMQTempTopic(): base()	{}
		public ActiveMQTempTopic(String name):base(name){}
		
		public String GetTopicName() 
		{
			return PhysicalName;
		}
		public override int GetDestinationType() 
		{
			return ACTIVEMQ_TOPIC;
		}

		public override ActiveMQDestination CreateDestination(String name) 
		{
			return new ActiveMQTempTopic(name);
		}

	}
}
