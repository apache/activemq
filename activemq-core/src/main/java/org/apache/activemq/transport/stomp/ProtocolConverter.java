/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.LongSequenceGenerator;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class ProtocolConverter {

    private static final IdGenerator connectionIdGenerator = new IdGenerator();
    private final ConnectionId connectionId = new ConnectionId(connectionIdGenerator.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);

    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator transactionIdGenerator = new LongSequenceGenerator();

    private final ConcurrentHashMap resposeHandlers = new ConcurrentHashMap();
    private final ConcurrentHashMap subscriptionsByConsumerId = new ConcurrentHashMap();
    private final Map transactions = new ConcurrentHashMap();
	private final StompTransportFilter transportFilter;

	private final Object commnadIdMutex = new Object();
	private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final FrameTranslator frameTranslator;

    public ProtocolConverter(StompTransportFilter stompTransportFilter, FrameTranslator translator)
    {
        this.transportFilter = stompTransportFilter;
        this.frameTranslator = translator;
    }

    protected int generateCommandId() {
    	synchronized(commnadIdMutex){
    		return lastCommandId++;
    	}
    }

    protected ResponseHandler createResponseHandler(StompFrame command){
        final String receiptId = (String) command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
        // A response may not be needed.
        if( receiptId != null ) {
	        return new ResponseHandler() {
	    		public void onResponse(ProtocolConverter converter, Response response) throws IOException {
	                StompFrame sc = new StompFrame();
	                sc.setAction(Stomp.Responses.RECEIPT);
	                sc.setHeaders(new HashMap(1));
	                sc.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
	        		transportFilter.sendToStomp(sc);
	    		}
	        };
	    }
    	return null;
    }

	protected void sendToActiveMQ(Command command, ResponseHandler handler) {
		command.setCommandId(generateCommandId());
		if(handler!=null) {
			command.setResponseRequired(true);
			resposeHandlers.put(new Integer(command.getCommandId()), handler);
		}
		transportFilter.sendToActiveMQ(command);
	}

	protected void sendToStomp(StompFrame command) throws IOException {
		transportFilter.sendToStomp(command);
	}

	/**
     * Convert a stomp command
     * @param command
     */
	public void onStompCommad( StompFrame command ) throws IOException, JMSException {
		try {

			if( command.getClass() == StompFrameError.class ) {
				throw ((StompFrameError)command).getException();
			}

			String action = command.getAction();
	        if (action.startsWith(Stomp.Commands.SEND))
	            onStompSend(command);
	        else if (action.startsWith(Stomp.Commands.ACK))
	            onStompAck(command);
	        else if (action.startsWith(Stomp.Commands.BEGIN))
	            onStompBegin(command);
	        else if (action.startsWith(Stomp.Commands.COMMIT))
	            onStompCommit(command);
	        else if (action.startsWith(Stomp.Commands.ABORT))
	            onStompAbort(command);
	        else if (action.startsWith(Stomp.Commands.SUBSCRIBE))
	            onStompSubscribe(command);
	        else if (action.startsWith(Stomp.Commands.UNSUBSCRIBE))
	            onStompUnsubscribe(command);
			else if (action.startsWith(Stomp.Commands.CONNECT))
	            onStompConnect(command);
	        else if (action.startsWith(Stomp.Commands.DISCONNECT))
	            onStompDisconnect(command);
	        else
	        	throw new ProtocolException("Unknown STOMP action: "+action);

        } catch (ProtocolException e) {

        	// Let the stomp client know about any protocol errors.
        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        	PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos,"UTF-8"));
        	e.printStackTrace(stream);
        	stream.close();

        	HashMap headers = new HashMap();
        	headers.put(Stomp.Headers.Error.MESSAGE, e.getMessage());

            final String receiptId = (String) command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
            if( receiptId != null ) {
            	headers.put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
            }

        	StompFrame errorMessage = new StompFrame(Stomp.Responses.ERROR,headers,baos.toByteArray());
			sendToStomp(errorMessage);

			if( e.isFatal() )
				getTransportFilter().onException(e);
        }
	}

	protected void onStompSend(StompFrame command) throws IOException, JMSException {
		checkConnected();

    	Map headers = command.getHeaders();
        String stompTx = (String) headers.get(Stomp.Headers.TRANSACTION);

        ActiveMQMessage message = convertMessage(command);

        message.setProducerId(producerId);
        MessageId id = new MessageId(producerId, messageIdGenerator.getNextSequenceId());
        message.setMessageId(id);
        message.setJMSTimestamp(System.currentTimeMillis());

        if (stompTx!=null) {
        	TransactionId activemqTx = (TransactionId) transactions.get(stompTx);
            if (activemqTx == null)
                throw new ProtocolException("Invalid transaction id: "+stompTx);
            message.setTransactionId(activemqTx);
        }

        message.onSend();
		sendToActiveMQ(message, createResponseHandler(command));

	}


    protected void onStompAck(StompFrame command) throws ProtocolException {
		checkConnected();

    	// TODO: acking with just a message id is very bogus
    	// since the same message id could have been sent to 2 different subscriptions
    	// on the same stomp connection. For example, when 2 subs are created on the same topic.

    	Map headers = command.getHeaders();
        String messageId = (String) headers.get(Stomp.Headers.Ack.MESSAGE_ID);
        if (messageId == null)
            throw new ProtocolException("ACK received without a message-id to acknowledge!");

        TransactionId activemqTx=null;
        String stompTx = (String) headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx!=null) {
        	activemqTx = (TransactionId) transactions.get(stompTx);
            if (activemqTx == null)
                throw new ProtocolException("Invalid transaction id: "+stompTx);
        }

        boolean acked=false;
        for (Iterator iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
			StompSubscription sub = (StompSubscription) iter.next();
			MessageAck ack = sub.onStompMessageAck(messageId);
			if( ack!=null ) {
		        ack.setTransactionId(activemqTx);
		        sendToActiveMQ(ack,createResponseHandler(command));
		        acked=true;
		        break;
			}
		}

        if( !acked )
        	throw new ProtocolException("Unexpected ACK received for message-id [" + messageId + "]");

	}


	protected void onStompBegin(StompFrame command) throws ProtocolException {
		checkConnected();

		Map headers = command.getHeaders();

        String stompTx = (String)headers.get(Stomp.Headers.TRANSACTION);

        if (!headers.containsKey(Stomp.Headers.TRANSACTION)) {
            throw new ProtocolException("Must specify the transaction you are beginning");
        }

        if( transactions.get(stompTx)!=null  ) {
            throw new ProtocolException("The transaction was allready started: "+stompTx);
        }

        LocalTransactionId activemqTx = new LocalTransactionId(connectionId, transactionIdGenerator.getNextSequenceId());
        transactions.put(stompTx, activemqTx);

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connectionId);
        tx.setTransactionId(activemqTx);
        tx.setType(TransactionInfo.BEGIN);

		sendToActiveMQ(tx, createResponseHandler(command));

	}

	protected void onStompCommit(StompFrame command) throws ProtocolException {
		checkConnected();

		Map headers = command.getHeaders();

        String stompTx = (String) headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx==null) {
            throw new ProtocolException("Must specify the transaction you are committing");
        }

        TransactionId activemqTx = (TransactionId) transactions.remove(stompTx);
        if (activemqTx == null) {
            throw new ProtocolException("Invalid transaction id: "+stompTx);
        }

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connectionId);
        tx.setTransactionId(activemqTx);
        tx.setType(TransactionInfo.COMMIT_ONE_PHASE);

		sendToActiveMQ(tx, createResponseHandler(command));
	}

	protected void onStompAbort(StompFrame command) throws ProtocolException {
		checkConnected();
    	Map headers = command.getHeaders();

        String stompTx = (String) headers.get(Stomp.Headers.TRANSACTION);
        if (stompTx==null) {
            throw new ProtocolException("Must specify the transaction you are committing");
        }

        TransactionId activemqTx = (TransactionId) transactions.remove(stompTx);
        if (activemqTx == null) {
            throw new ProtocolException("Invalid transaction id: "+stompTx);
        }

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connectionId);
        tx.setTransactionId(activemqTx);
        tx.setType(TransactionInfo.ROLLBACK);

		sendToActiveMQ(tx, createResponseHandler(command));

	}

	protected void onStompSubscribe(StompFrame command) throws ProtocolException {
		checkConnected();
    	Map headers = command.getHeaders();

        String subscriptionId = (String)headers.get(Stomp.Headers.Subscribe.ID);
        String destination = (String)headers.get(Stomp.Headers.Subscribe.DESTINATION);

        ActiveMQDestination actual_dest = frameTranslator.convertDestination(destination);
        ConsumerId id = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setPrefetchSize(1000);
        consumerInfo.setDispatchAsync(true);

        String selector = (String) headers.remove(Stomp.Headers.Subscribe.SELECTOR);
        consumerInfo.setSelector(selector);

        IntrospectionSupport.setProperties(consumerInfo, headers, "activemq.");

        consumerInfo.setDestination(frameTranslator.convertDestination(destination));

        StompSubscription stompSubscription = new StompSubscription(this, subscriptionId, consumerInfo);
        stompSubscription.setDestination(actual_dest);

        String ackMode = (String)headers.get(Stomp.Headers.Subscribe.ACK_MODE);
        if (Stomp.Headers.Subscribe.AckModeValues.CLIENT.equals(ackMode)) {
            stompSubscription.setAckMode(StompSubscription.CLIENT_ACK);
        } else {
            stompSubscription.setAckMode(StompSubscription.AUTO_ACK);
        }

        subscriptionsByConsumerId.put(id, stompSubscription);
		sendToActiveMQ(consumerInfo, createResponseHandler(command));

	}

	protected void onStompUnsubscribe(StompFrame command) throws ProtocolException {
		checkConnected();
    	Map headers = command.getHeaders();

        ActiveMQDestination destination=null;
        Object o = headers.get(Stomp.Headers.Unsubscribe.DESTINATION);
        if( o!=null )
        	destination = frameTranslator.convertDestination((String) o);

        String subscriptionId = (String)headers.get(Stomp.Headers.Unsubscribe.ID);

        if (subscriptionId==null && destination==null) {
            throw new ProtocolException("Must specify the subscriptionId or the destination you are unsubscribing from");
        }

        // TODO: Unsubscribing using a destination is a bit wierd if multiple subscriptions
        // are created with the same destination.  Perhaps this should be removed.
        //
        for (Iterator iter = subscriptionsByConsumerId.values().iterator(); iter.hasNext();) {
			StompSubscription sub = (StompSubscription) iter.next();
			if (
				(subscriptionId!=null && subscriptionId.equals(sub.getSubscriptionId()) ) ||
				(destination!=null && destination.equals(sub.getDestination()) )
			) {
		        sendToActiveMQ(sub.getConsumerInfo().createRemoveCommand(), createResponseHandler(command));
				iter.remove();
                return;
			}
		}

        throw new ProtocolException("No subscription matched.");
	}

	protected void onStompConnect(StompFrame command) throws ProtocolException {

		if(connected.get()) {
			throw new ProtocolException("Allready connected.");
		}

    	final Map headers = command.getHeaders();

        // allow anyone to login for now
        String login = (String)headers.get(Stomp.Headers.Connect.LOGIN);
        String passcode = (String)headers.get(Stomp.Headers.Connect.PASSCODE);
        String clientId = (String)headers.get(Stomp.Headers.Connect.CLIENT_ID);

        final ConnectionInfo connectionInfo = new ConnectionInfo();

        IntrospectionSupport.setProperties(connectionInfo, headers, "activemq.");

        connectionInfo.setConnectionId(connectionId);
        if( clientId!=null )
            connectionInfo.setClientId(clientId);
        else
            connectionInfo.setClientId(""+connectionInfo.getConnectionId().toString());

        connectionInfo.setResponseRequired(true);
        connectionInfo.setUserName(login);
        connectionInfo.setPassword(passcode);

		sendToActiveMQ(connectionInfo, new ResponseHandler(){
			public void onResponse(ProtocolConverter converter, Response response) throws IOException {

	            final SessionInfo sessionInfo = new SessionInfo(sessionId);
	            sendToActiveMQ(sessionInfo,null);


	            final ProducerInfo producerInfo = new ProducerInfo(producerId);
	            sendToActiveMQ(producerInfo,new ResponseHandler(){
					public void onResponse(ProtocolConverter converter, Response response) throws IOException {

						connected.set(true);
	                    HashMap responseHeaders = new HashMap();

	                    responseHeaders.put(Stomp.Headers.Connected.SESSION, connectionInfo.getClientId());
	                    String requestId = (String) headers.get(Stomp.Headers.Connect.REQUEST_ID);
	                    if( requestId !=null ){
		                    responseHeaders.put(Stomp.Headers.Connected.RESPONSE_ID, requestId);
	            		}

	                    StompFrame sc = new StompFrame();
	                    sc.setAction(Stomp.Responses.CONNECTED);
	                    sc.setHeaders(responseHeaders);
	                    sendToStomp(sc);
					}
				});

			}
		});

	}

	protected void onStompDisconnect(StompFrame command) throws ProtocolException {
		checkConnected();
		sendToActiveMQ(new ShutdownInfo(), createResponseHandler(command));
		connected.set(false);
	}


	protected void checkConnected() throws ProtocolException {
		if(!connected.get()) {
			throw new ProtocolException("Not connected.");
		}
	}

	/**
     * Dispatch a ActiveMQ command
     * @param command
     * @throws IOException
     */
	public void onActiveMQCommad( Command command ) throws IOException, JMSException {

    	if ( command.isResponse() ) {

			Response response = (Response) command;
		    ResponseHandler rh = (ResponseHandler) resposeHandlers.remove(new Integer(response.getCorrelationId()));
		    if( rh !=null ) {
		    	rh.onResponse(this, response);
		    }

		} else if( command.isMessageDispatch() ) {

		    MessageDispatch md = (MessageDispatch)command;
		    StompSubscription sub = (StompSubscription) subscriptionsByConsumerId.get(md.getConsumerId());
		    if (sub != null) {
		        sub.onMessageDispatch(md);
            }
        }
	}

    public  ActiveMQMessage convertMessage(StompFrame command) throws IOException, JMSException {

        ActiveMQMessage msg = frameTranslator.convertFrame(command);

        return msg;
    }

	public StompFrame convertMessage(ActiveMQMessage message) throws IOException, JMSException {
		return frameTranslator.convertMessage(message);
    }

	public StompTransportFilter getTransportFilter() {
		return transportFilter;
	}
}
