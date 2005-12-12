using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for ProducerInfo.
	/// </summary>
	public class ProducerInfo : AbstractPacket 
	{
		private ActiveMQDestination destination;
		private short producerId;
		private String clientId;
		private short sessionId;
		private long startTime;
		private bool started;


		/**
		 * Test for equality
		 *
		 * @param obj object to test
		 * @return true if equivalent
		 */
		public override bool Equals(Object obj) 
		{
			bool result = false; 
			if (obj != null && obj is ProducerInfo) 
			{
				ProducerInfo info = (ProducerInfo) obj;
				result = this.producerId == info.producerId &&
					this.sessionId == info.sessionId &&
					this.clientId.equals(info.clientId);
			}
			return result;
		}

		/**
		 * @return hash code for instance
		 */
    
		public override int GetHashCode() 
		{
			if (cachedHashCode == -1)
			{
				String hashCodeStr = clientId + sessionId + producerId;
				cachedHashCode = hashCodeStr.hashCode();
			}
			return cachedHashCode;
		}


		/**
		 * @return Returns the producerId.
		 */
		public short getProducerId() 
		{
			return producerId;
		}

		/**
		 * @param producerId The producerId to set.
		 */
		public void setProducerId(short producerId) 
		{
			this.producerId = producerId;
		}

		/**
		 * @return Returns the sessionId.
		 */
		public short getSessionId() 
		{
			return sessionId;
		}

		/**
		 * @param sessionId The sessionId to set.
		 */
		public void setSessionId(short sessionId) 
		{
			this.sessionId = sessionId;
		}

		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return PRODUCER_INFO;
		}


		/**
		 * @return Returns the clientId.
		 */
		public String getClientId() 
		{
			return this.clientId;
		}

		/**
		 * @param newClientId The clientId to set.
		 */
		public void setClientId(String newClientId) 
		{
			this.clientId = newClientId;
		}


		/**
		 * @return Returns the destination.
		 */
		public ActiveMQDestination getDestination() 
		{
			return this.destination;
		}

		/**
		 * @param newDestination The destination to set.
		 */
		public void setDestination(ActiveMQDestination newDestination) 
		{
			this.destination = newDestination;
		}


		/**
		 * @return Returns the started.
		 */
		public bool isStarted() 
		{
			return this.started;
		}

		/**
		 * @param flag to indicate if started
		 */
		public void setStarted(bool flag) 
		{
			this.started = flag;
		}

		/**
		 * @return Returns the startTime.
		 */
		public long getStartTime() 
		{
			return this.startTime;
		}

		/**
		 * @param newStartTime The startTime to set.
		 */
		public void setStartTime(long newStartTime) 
		{
			this.startTime = newStartTime;
		}

		public override String ToString() 
		{
			return super.toString() + " ProducerInfo{ " +
				"clientId = '" + clientId + "' " +
				", destination = " + destination +
				", producerId = '" + producerId + "' " +
				", sessionId = '" + sessionId + "' " +
				", startTime = " + startTime +
				", started = " + started +
				" }";
		}

	}
}
