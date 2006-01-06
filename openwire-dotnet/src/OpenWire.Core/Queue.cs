using System;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for Queue.
	/// </summary>
	public class Queue : Destination {
		public Queue() : base(){}
		public Queue(String name) : base(name){}
		
		public String GetQueueName() {
			return base.GetPhysicalName();
		}
		
		public override int GetDestinationType() {
			return ACTIVEMQ_QUEUE;
		}

		public override ActiveMQDestination CreateDestination(String name) {
			return new ActiveMQQueue(name);
		}
	}
}