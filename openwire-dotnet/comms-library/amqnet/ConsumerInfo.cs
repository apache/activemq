using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for ConsumerInfo.
	/// </summary>
	public class ConsumerInfo : AbstractPacket {
													
		private ActiveMQDestination destination;
		private String clientId;
		private short sessionId;
		private String consumerName;
		private String selector;
		private long startTime;
		private bool started;
		private int consumerNo;
		private bool noLocal;
		private bool browser;
		private int prefetchNumber = 100;
		
		private String consumerId;

    
    
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
			return CONSUMER_INFO;
		}

		/**
		 * Test for equality
		 *
		 * @param obj object to test
		 * @return true if equivalent
		 */
		public bool equals(Object obj) 
		{
			bool result = false;
			if (obj != null && obj is ConsumerInfo) 
														  {
																ConsumerInfo that = (ConsumerInfo) obj;
																result = this.getConsumerId().equals(that.getConsumerId());
															}
			return result;
		}

		/**
		 * @return hash code for instance
		 */
		public new int hashCode() 
		{
			return getConsumerId().hashCode();
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
		 * @return Returns the selector.
		 */
		public String getSelector() 
		{
			return this.selector;
		}

		/**
		 * @param newSelector The selector to set.
		 */
		public void setSelector(String newSelector) 
		{
			this.selector = newSelector;
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

		/**
		 * @return Returns the consumerNo.
		 */
		public int getConsumerNo() 
		{
			return this.consumerNo;
		}

		/**
		 * @param newConsumerNo The consumerNo to set.
		 */
		public void setConsumerNo(int newConsumerNo) 
		{
			this.consumerNo = newConsumerNo;
		}

		/**
		 * @return Returns the consumer name.
		 */
		public String getConsumerName() 
		{
			return this.consumerName;
		}

		/**
		 * @param newconsumerName The consumerName to set.
		 */
		public void setConsumerName(String newconsumerName) 
		{
			this.consumerName = newconsumerName;
		}

		/**
		 * @return Returns true if the Consumer is a durable Topic subscriber
		 */
		public bool isDurableTopic() 
		{
			return this.destination.isTopic() && !this.destination.isTemporary() && this.consumerName != null
				&& this.consumerName.length() > 0;
		}

		/**
		 * @return Returns the noLocal.
		 */
		public bool isNoLocal() 
		{
			return noLocal;
		}

		/**
		 * @param noLocal The noLocal to set.
		 */
		public void setNoLocal(bool noLocal) 
		{
			this.noLocal = noLocal;
		}

		/**
		 * @return Returns the browser.
		 */
		public bool isBrowser() 
		{
			return browser;
		}

		/**
		 * @param browser The browser to set.
		 */
		public void setBrowser(bool browser) 
		{
			this.browser = browser;
		}

		/**
		 * @return Returns the prefetchNumber.
		 */
		public int getPrefetchNumber() 
		{
			return prefetchNumber;
		}

		/**
		 * @param prefetchNumber The prefetchNumber to set.
		 */
		public void setPrefetchNumber(int prefetchNumber) 
		{
			this.prefetchNumber = prefetchNumber;
		}


		/**
		 * Creates a primary key for the consumer info which uniquely
		 * describes the consumer using a combination of clientID and
		 * consumerName
		 *
		 * @return the consumerKey
		 */
		public String getConsumerKey() 
		{
			if (consumerKey == null)
			{
				consumerKey = generateConsumerKey(clientId,consumerName);
			}
			return consumerKey;
		}
    
		/**
		 * @return Returns the consumerIdentifier.
		 */
		public String getConsumerId() 
		{
			if (consumerId == null)
			{
				consumerId = clientId + "." + sessionId + "." + consumerNo;
			}
			return consumerId;
		}
		/**
		 * @param consumerIdentifier The consumerIdentifier to set.
		 */
		public void setConsumerId(String consumerIdentifier) 
		{
			this.consumerId = consumerIdentifier;
		}
    
    
		/**
		 * Generate a primary key for a consumer from the clientId and consumerName
		 * @param clientId
		 * @param consumerName
		 * @return
		 */
		public static String generateConsumerKey(String clientId, String consumerName)
		{
			return "[" + clientId + ":" + consumerName + "]";
		}

		/**
		 * @return true if the consumer is interested in advisory messages
		 */
		public bool isAdvisory()
		{
			return destination != null && destination.isAdvisory();
		}
    
		/**
		 * @return a pretty print
		 */
		public override String ToString() 
		{
			return super.toString() + " ConsumerInfo{ " +
				"browser = " + browser +
				", destination = " + destination +
				", consumerIdentifier = '" + getConsumerId() + "' " +
				", clientId = '" + clientId + "' " +
				", sessionId = '" + sessionId + "' " +
				", consumerName = '" + consumerName + "' " +
				", selector = '" + selector + "' " +
				", startTime = " + startTime +
				", started = " + started +
				", consumerNo = " + consumerNo +
				", noLocal = " + noLocal +
				", prefetchNumber = " + prefetchNumber +
				", consumerKey = '" + getConsumerKey() + "' " +
				" }";
		}
	}

}
