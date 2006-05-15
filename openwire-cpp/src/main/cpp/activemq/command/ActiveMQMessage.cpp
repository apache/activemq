/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "activemq/command/ActiveMQMessage.hpp"

#include "ppr/io/ByteArrayOutputStream.hpp"
#include "ppr/io/ByteArrayInputStream.hpp"
#include "ppr/io/DataOutputStream.hpp"
#include "ppr/io/DataInputStream.hpp"

using namespace apache::activemq::command;


// Constructors -----------------------------------------------------


// Attribute methods ------------------------------------------------

/*
 * 
 */
unsigned char ActiveMQMessage::getDataStructureType()
{
    return ActiveMQMessage::TYPE ;
}

/*
 * 
 */
p<IDestination> ActiveMQMessage::getFromDestination()
{
    return this->destination ;
}

/*
 * 
 */
void ActiveMQMessage::setFromDestination(p<IDestination> destination)
{
    this->destination = p_dyncast<ActiveMQDestination> (destination) ;
}

/*
 * 
 */
void ActiveMQMessage::setAcknowledger(p<IAcknowledger> acknowledger) 
{
    this->acknowledger = acknowledger ;
}

/*
 * 
 */
p<PropertyMap> ActiveMQMessage::getProperties()
{
    if( properties == NULL )
        properties = new PropertyMap() ;

    return properties;
}

/*
 * 
 */
p<string> ActiveMQMessage::getJMSCorrelationID()
{
    return this->correlationId ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSCorrelationID(const char* correlationId)
{
    this->correlationId = new string(correlationId) ;
}

/*
 * 
 */
p<IDestination> ActiveMQMessage::getJMSDestination()
{
    return this->originalDestination ;
}

/*
 * 
 */
long long ActiveMQMessage::getJMSExpiration()
{
    return this->expiration ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSExpiration(long long time)
{
    this->expiration = time ;
}

/*
 * 
 */
p<string> ActiveMQMessage::getJMSMessageID()
{
    p<string>     str ;
    p<ProducerId> pid = this->getMessageId()->getProducerId() ;
    char          buffer[256] ;

    // Compose message id as a string
#ifdef unix
    sprintf(buffer, "%s:%lld:%lld:%lld", pid->getConnectionId()->c_str(),
                                          pid->getSessionId(),
                                          pid->getValue(),
                                          messageId->getProducerSequenceId() ) ;
#else
    sprintf(buffer, "%s:%I64d:%I64d:%I64d", pid->getConnectionId()->c_str(),
                                            pid->getSessionId(),
                                            pid->getValue(),
                                            messageId->getProducerSequenceId() ) ;
#endif

    str = new string(buffer) ;
    return str ;
}

/*
 * 
 */
bool ActiveMQMessage::getJMSPersistent()
{
    return this->persistent ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSPersistent(bool persistent)
{
    this->persistent = persistent ;
}

/*
 * 
 */
unsigned char ActiveMQMessage::getJMSPriority()
{
    return this->priority ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSPriority(unsigned char priority)
{
    this->priority = priority ;
}

/*
 * 
 */
bool ActiveMQMessage::getJMSRedelivered()
{
    return ( this->redeliveryCounter > 0 ) ? true : false ;
}

/*
 * 
 */
p<IDestination> ActiveMQMessage::getJMSReplyTo()
{
    return this->replyTo ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSReplyTo(p<IDestination> destination)
{
    this->replyTo = p_dyncast<ActiveMQDestination> (destination) ;

}

/*
 * 
 */
long long ActiveMQMessage::getJMSTimestamp()
{
    return this->timestamp ;
}

/*
 * 
 */
p<string> ActiveMQMessage::getJMSType()
{
    return this->type ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSType(const char* type)
{
    this->type = new string(type) ;
}

/*
 * 
 */
int ActiveMQMessage::getJMSXDeliveryCount()
{
    return this->redeliveryCounter + 1 ;
}

/*
 * 
 */
p<string> ActiveMQMessage::getJMSXGroupID()
{
    return this->groupID ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSXGroupID(const char* groupId)
{
    this->groupID = new string(groupId) ;
}

/*
 * 
 */
int ActiveMQMessage::getJMSXGroupSeq()
{
    return this->groupSequence ;
}

/*
 * 
 */
void ActiveMQMessage::setJMSXGroupSeq(int sequence)
{
    this->groupSequence = sequence ;
}

/*
 * 
 */
p<string> ActiveMQMessage::getJMSXProducerTxID()
{
    p<TransactionId> txId = this->originalTransactionId ;
    p<string>        str ;
    
    if( txId == NULL )
        txId = this->transactionId ;

    if( txId != NULL )
    {
        if( txId->getDataStructureType() == LocalTransactionId::TYPE )
        {
            p<LocalTransactionId> localTxId = p_cast<LocalTransactionId> (txId) ;
            char buffer[256] ;

            // Compose local transaction id string
#ifdef unix
            sprintf(buffer, "%lld", localTxId->getValue() ) ;
#else
            sprintf(buffer, "%I64d", localTxId->getValue() ) ;
#endif

            str = new string(buffer ) ;
            return str  ;
        }
        else if( txId->getDataStructureType() == XATransactionId::TYPE )
        {
            p<XATransactionId> xaTxId = p_cast<XATransactionId> (txId) ;
            char buffer[256] ;

            // Compose XA transaction id string
            sprintf(buffer, "XID:%d:%s:%s", xaTxId->getFormatId(),
                                            Hex::toString( xaTxId->getGlobalTransactionId() )->c_str(),
                                            Hex::toString( xaTxId->getBranchQualifier() )->c_str() ) ;

            str = new string(buffer ) ;
            return str  ;
        }
        return NULL ;
    }
    return NULL ;
}


// Operation methods ------------------------------------------------

/*
 *
 */
void ActiveMQMessage::acknowledge()
{
    if( acknowledger != NULL )
        acknowledger->acknowledge(smartify(this)) ;
}


// Implementation methods -------------------------------------------

/*
 * 
 */
int ActiveMQMessage::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw(IOException)
{
    int size = 0 ;

    // Update message content if available
    if( mode == IMarshaller::MARSHAL_SIZE && this->properties != NULL )
    {
        p<ByteArrayOutputStream> bos = new ByteArrayOutputStream() ;
        p<DataOutputStream>      dos = new DataOutputStream( bos ) ;

        // Marshal properties into a byte array
        marshaller->marshalMap(properties, IMarshaller::MARSHAL_WRITE, dos) ;

        // Store properties byte array in message content
        this->marshalledProperties = bos->toArray() ;
    }
    // Note! Message propertys marshalling is done in super class
    size += Message::marshal(marshaller, mode, ostream) ;

    return size ;
}

/*
 * 
 */
void ActiveMQMessage::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw(IOException)
{
    // Note! Message property unmarshalling is done in super class
    Message::unmarshal(marshaller, mode, istream) ;

    // Extract properties from message
    if( mode == IMarshaller::MARSHAL_READ )
    {
        if( this->marshalledProperties != NULL )
        {
            p<ByteArrayInputStream> bis = new ByteArrayInputStream( this->marshalledProperties ) ;
            p<DataInputStream>      dis = new DataInputStream( bis ) ;

            // Unmarshal map into a map
            properties = marshaller->unmarshalMap(mode, dis) ;
        }
    }
}
