using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for Receipt.
	/// </summary>
	public class Receipt : AbstractPacket 
	{

		private short correlationId;
		private String brokerName;
		private String clusterName;
		private String exception;
		private bool failed;
		private int brokerMessageCapacity = 100;


		/**
		 * @return Returns the jmsException.
		 */
		public String getException() 
		{
			return exception;
		}

		/**
		 * @param exception The exception to set.
		 */
		public void setException(String exception) 
		{
			this.exception = exception;
		}

		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return RECEIPT_INFO;
		}

		/**
		 * @return true, this is a receipt packet
		 */
		public new bool isReceipt() 
		{
			return true;
		}

		/**
		 * @return Returns the correlationId.
		 */
		public short getCorrelationId() 
		{
			return this.correlationId;
		}

		/**
		 * @param newCorrelationId The correlationId to set.
		 */
		public void setCorrelationId(short newCorrelationId) 
		{
			this.correlationId = newCorrelationId;
		}

		/**
		 * @return Returns the failed.
		 */
		public bool isFailed() 
		{
			return this.failed;
		}

		/**
		 * @param newFailed The failed to set.
		 */
		public void setFailed(bool newFailed) 
		{
			this.failed = newFailed;
		}

		/**
		 * @return Returns the brokerMessageCapacity.
		 */
		public int getBrokerMessageCapacity() 
		{
			return brokerMessageCapacity;
		}
		/**
		 * @param brokerMessageCapacity The brokerMessageCapacity to set.
		 */
		public void setBrokerMessageCapacity(int brokerMessageCapacity) 
		{
			this.brokerMessageCapacity = brokerMessageCapacity;
		}
		/**
		 * @return Returns the brokerName.
		 */
		public String getBrokerName() 
		{
			return brokerName;
		}
		/**
		 * @param brokerName The brokerName to set.
		 */
		public void setBrokerName(String brokerName) 
		{
			this.brokerName = brokerName;
		}
		/**
		 * @return Returns the clusterName.
		 */
		public String getClusterName() 
		{
			return clusterName;
		}
		/**
		 * @param clusterName The clusterName to set.
		 */
		public void setClusterName(String clusterName) 
		{
			this.clusterName = clusterName;
		}

		/**
		 * @return pretty print of a Receipt
		 */
		public override String ToString() 
		{
			return super.toString() + " Receipt{ " +
				"brokerMessageCapacity = " + brokerMessageCapacity +
				", correlationId = '" + correlationId + "' " +
				", brokerName = '" + brokerName + "' " +
				", clusterName = '" + clusterName + "' " +
				", exception = " + exception +
				", failed = " + failed +
				" }";
		}
	}
}
