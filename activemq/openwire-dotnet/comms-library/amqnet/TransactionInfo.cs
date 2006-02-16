using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for TransactionInfo.
	/// </summary>
	public class TransactionInfo : AbstractPacket {

		private int type;
		private String transactionId;

		/**
		 * @return Returns the transactionId.
		 */
		public String getTransactionId() 
		{
			return transactionId;
		}

		/**
		 * @param transactionId The transactionId to set.
		 */
		public void setTransactionId(String transactionId) 
		{
			this.transactionId = transactionId;
		}

		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return TRANSACTION_INFO;
		}


		/**
		 * Test for equality
		 *
		 * @param obj object to test
		 * @return true if equivalent
		 */
		public bool equals(Object obj) 
		{
			boolean result = false;
			if (obj != null && obj is TransactionInfo) {
				TransactionInfo info = (TransactionInfo) obj;
				result = this.transactionId == info.transactionId;
			}
			return result;
		}

		/**
		 * @return hash code for instance
		 */
		public override int GetHashCode() 
		{
			return this.transactionId != null ? this.transactionId.hashCode() : base.hashCode();
		}

		/**
		 * @return Returns the type of transacton command.
		 */
		public int getType() 
		{
			return this.type;
		}

		/**
		 * @param newType the type of transaction command The type to set.
		 */
		public void setType(int newType) 
		{
			this.type = newType;
		}

		public override String ToString() 
		{
			return base.toString() + " TransactionInfo{ " +
				"transactionId = '" + transactionId + "' " +
				", type = " + type +
				" }";
		}
	}

}
