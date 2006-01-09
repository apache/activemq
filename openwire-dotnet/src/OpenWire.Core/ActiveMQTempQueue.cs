using System;
using OpenWire.Core;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for ActiveMQTempQueue.
	/// </summary>
	public class ActiveMQTempQueue : ActiveMQDestination, TemporaryQueue
	{
		public const byte ID_ActiveMQTempQueue = 102;
		
		public ActiveMQTempQueue() : base(){}
		public ActiveMQTempQueue(String name) : base(name){}
		
		public String GetQueueName() {
			return PhysicalName;
		}
		
		public override int GetDestinationType() {
			return ACTIVEMQ_QUEUE;
		}

		public override ActiveMQDestination CreateDestination(String name) {
			return new ActiveMQTempQueue(name);
		}
	}
}