using System;
using System.Collections;
namespace ActiveMQ
{
	public class ConnectionInfo : AbstractPacket
	{
			
		public static String NO_DELAY_PROPERTY = "noDelay";
		String clientId;
		String userName;
		String password;
		String hostName;
		String clientVersion;
		int wireFormatVersion;
		long startTime;
		bool started;
		bool closed;
		Hashtable properties = new Hashtable();
    
		public override short getPacketType() 
		{
			return PacketConstants.ACTIVEMQ_CONNECTION_INFO;
		}

		public override bool Equals(Object obj) 
		{
			bool result = false;
			if (obj != null && obj is ConnectionInfo) 
			{
				ConnectionInfo info = (ConnectionInfo) obj;
				result = this.clientId == info.clientId;
			}
			return result;
		}
		public override int GetHashCode() 
		{
			return this.clientId != null ? this.clientId.GetHashCode() : base.GetHashCode();
		}


		public String getClientId() 
		{
			return this.clientId;
		}

		public void setClientId(String newClientId) 
		{
			this.clientId = newClientId;
		}

		public String getHostName() 
		{
			return this.hostName;
		}

		public void setHostName(String newHostName) 
		{
			this.hostName = newHostName;
		}

		public String getPassword() 
		{
			return this.password;
		}

		
		public void setPassword(String newPassword) 
		{
			this.password = newPassword;
		}

		public Hashtable getProperties() 
		{
			return this.properties;
		}

		
		public void setProperties(Hashtable newProperties) 
		{
			this.properties = newProperties;
		}

		public long getStartTime() 
		{
			return this.startTime;
		}

		public void setStartTime(long newStartTime) 
		{
			this.startTime = newStartTime;
		}

		public String getUserName() 
		{
			return this.userName;
		}

		public void setUserName(String newUserName) 
		{
			this.userName = newUserName;
		}

		public bool isStarted() 
		{
			return started;
		}

		public void setStarted(bool started) 
		{
			this.started = started;
		}

		public bool isClosed() 
		{
			return closed;
		}

		public void setClosed(bool closed) 
		{
			this.closed = closed;
		}
		
		public String getClientVersion() 
		{
			return clientVersion;
		}
		
		public void setClientVersion(String clientVersion) 
		{
			this.clientVersion = clientVersion;

		}
		
		public int getWireFormatVersion() 
		{
			return wireFormatVersion;
		}
		
		public void setWireFormatVersion(int wireFormatVersion) 
		{
			this.wireFormatVersion = wireFormatVersion;
		}


		public override String ToString() 
		{
			return base.ToString() + " ConnectionInfo{ " +
				"clientId = '" + clientId + "' " +
				", userName = '" + userName + "' " +
				", hostName = '" + hostName + "' " +
				", clientVersion = '" + clientVersion + "' " +
				", wireFormatVersion = " + wireFormatVersion +
				", startTime = " + startTime +
				", started = " + started +
				", closed = " + closed +
				", properties = " + properties +
				" }";
		}    
	}
}

	

