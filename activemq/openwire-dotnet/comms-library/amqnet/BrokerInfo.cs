using System;
using System.Collections;
namespace ActiveMQ
{
	public class BrokerInfo : AbstractPacket {

		private String brokerName;
		private String clusterName;
		private long startTime;
		private Hashtable properties;
		private bool remote;

    
		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return ACTIVEMQ_BROKER_INFO;
		}


		/**
		 * @return Returns the brokerName.
		 */
		public String getBrokerName() 
		{
			return this.brokerName;
		}

		/**
		 * @param newBrokerName The brokerName to set.
		 */
		public void setBrokerName(String newBrokerName) 
		{
			this.brokerName = newBrokerName;
		}

		/**
		 * @return Returns the clusterName.
		 */
		public String getClusterName() 
		{
			return this.clusterName;
		}

		/**
		 * @param newClusterName The clusterName to set.
		 */
		public void setClusterName(String newClusterName) 
		{
			this.clusterName = newClusterName;
		}

		/**
		 * @return Returns the properties.
		 */
		public Hashtable getProperties() 
		{
			return this.properties;
		}

		/**
		 * @param newProperties The properties to set.
		 */
		public void setProperties(Hashtable newProperties) 
		{
			this.properties = newProperties;
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
    
		/**
		 * @return Returns the boondocks.
		 */
		public bool isRemote() 
		{
			return remote;
		}
		/**
		 * @param boondocks The boondocks to set.
		 */
		public void setRemote(bool boondocks) 
		{
			this.remote = boondocks;
		}


		public override String ToString() 
		{
			return base.ToString() + " BrokerInfo{ " +
				"brokerName = '" + brokerName + "' " +
				", clusterName = '" + clusterName + "' " +
				", startTime = " + startTime +
				", properties = " + properties +
				" }";
		}
	}

}
