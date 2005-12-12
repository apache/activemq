using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for SessionInfo.
	/// </summary>
	public class SessionInfo : AbstractPacket {

		private String clientId;
		private short sessionId;
		private long startTime;
		private bool started;
		private int sessionMode;

		/**
		 * @return Returns the sessionMode.
		 */
		public int getSessionMode() 
		{
			return sessionMode;
		}
		/**
		 * @param sessionMode The sessionMode to set.
		 */
		public void setSessionMode(int sessionMode) 
		{
			this.sessionMode = sessionMode;
		}
		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return SESSION_INFO;
		}


		/**
		 * Test for equality
		 *
		 * @param obj object to test
		 * @return true if equivalent
		 */
		public override bool Equals(Object obj) 
		{
			boolean result = false;
			if (obj != null && obj is SessionInfo) {
				SessionInfo info = (SessionInfo) obj;
				result = this.clientId.equals(info.clientId) && this.sessionId == info.sessionId;
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
				String hashCodeStr = clientId + sessionId;
				cachedHashCode = hashCodeStr.hashCode();
			}
			return cachedHashCode;
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
			return super.toString() + " SessionInfo{ " +
				"clientId = '" + clientId + "' " +
				", sessionId = '" + sessionId + "' " +
				", startTime = " + startTime +
				", started = " + started +
				" }";
		}
	}

}
