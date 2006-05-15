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
#include "activemq/DestinationFilter.hpp"
#include "activemq/command/ActiveMQDestination.hpp"
#include "activemq/command/ActiveMQTempQueue.hpp"
#include "activemq/command/ActiveMQTempTopic.hpp"
#include "activemq/command/ActiveMQQueue.hpp"
#include "activemq/command/ActiveMQTopic.hpp"

using namespace apache::activemq::command;


// --- Static initialization ----------------------------------------

const char* ActiveMQDestination::ADVISORY_PREFIX            = "ActiveMQ.Advisory." ;
const char* ActiveMQDestination::CONSUMER_ADVISORY_PREFIX   = "ActiveMQ.Advisory.Consumers." ;
const char* ActiveMQDestination::PRODUCER_ADVISORY_PREFIX   = "ActiveMQ.Advisory.Producers." ;
const char* ActiveMQDestination::CONNECTION_ADVISORY_PREFIX = "ActiveMQ.Advisory.Connections." ;
const char* ActiveMQDestination::DEFAULT_ORDERED_TARGET     = "coordinator" ;

const char* ActiveMQDestination::TEMP_PREFIX         = "{TD{" ;
const char* ActiveMQDestination::TEMP_POSTFIX        = "}TD}" ;
const char* ActiveMQDestination::COMPOSITE_SEPARATOR = "," ;
const char* ActiveMQDestination::QUEUE_PREFIX        = "queue://" ;
const char* ActiveMQDestination::TOPIC_PREFIX        = "topic://" ;


/*
 * Default constructor
 */
ActiveMQDestination::ActiveMQDestination()
{
    orderedTarget = new string(DEFAULT_ORDERED_TARGET) ;
    physicalName  = new string("") ;
    exclusive     = false ;
    ordered       = false ;
    advisory      = false ;
}

/*
 * Constructs the destination with a specified physical name.
 *
 * @param   name the physical name for the destination.
 */
ActiveMQDestination::ActiveMQDestination(const char* name)
{
    orderedTarget = new string(DEFAULT_ORDERED_TARGET) ;
    physicalName  = new string(name) ;
    exclusive     = false ;
    ordered       = false ;
    advisory      = ( name != NULL && (physicalName->find(ADVISORY_PREFIX) == 0)) ? true : false ;
}

/*
 *
 */
ActiveMQDestination::~ActiveMQDestination()
{
    // no-op
}

/*
 *
 */
bool ActiveMQDestination::isAdvisory()
{
    return advisory ;
}

/*
 *
 */
void ActiveMQDestination::setAdvisory(bool advisory)
{
    this->advisory = advisory ;
}

/*
 *
 */
bool ActiveMQDestination::isConsumerAdvisory()
{
    return ( isAdvisory() && (physicalName->find(CONSUMER_ADVISORY_PREFIX) == 0) ) ;
}

/*
 *
 */
bool ActiveMQDestination::isProducerAdvisory()
{
    return ( isAdvisory() && (physicalName->find(PRODUCER_ADVISORY_PREFIX) == 0) ) ;
}

/*
 *
 */
bool ActiveMQDestination::isConnectionAdvisory()
{
    return ( isAdvisory() && (physicalName->find(CONNECTION_ADVISORY_PREFIX) == 0) ) ;
}

/*
 *
 */
bool ActiveMQDestination::isExclusive()
{
    return exclusive ;
}

/*
 *
 */
void ActiveMQDestination::setExclusive(bool exclusive)
{
    this->exclusive = exclusive ;
}

/*
 *
 */
bool ActiveMQDestination::isOrdered()
{
    return ordered ;
}

/*
 *
 */
void ActiveMQDestination::setOrdered(bool ordered)
{
    this->ordered = ordered ;
}

/*
 *
 */
p<string> ActiveMQDestination::getOrderedTarget()
{
    return orderedTarget ;
}

/*
 *
 */
void ActiveMQDestination::setOrderedTarget(const char* target)
{
    this->orderedTarget->assign(target) ;
}

/*
 *
 */
p<string> ActiveMQDestination::getPhysicalName()
{
    return physicalName ;
}

/*
 *
 */
void ActiveMQDestination::setPhysicalName(const char* name)
{
    physicalName->assign(name) ;
}

/*
 *
 */
bool ActiveMQDestination::isTopic()
{
    return ( getDestinationType() == ACTIVEMQ_TOPIC ||
             getDestinationType() == ACTIVEMQ_TEMPORARY_TOPIC ) ; 
}

/*
 *
 */
bool ActiveMQDestination::isQueue()
{
    return !isTopic() ;
}

/*
 *
 */
bool ActiveMQDestination::isTemporary()
{
    return ( getDestinationType() == ACTIVEMQ_TEMPORARY_TOPIC ||
             getDestinationType() == ACTIVEMQ_TEMPORARY_QUEUE ) ; 
}

/*
 *
 */
bool ActiveMQDestination::isComposite()
{
    return ( physicalName->find(ActiveMQDestination::COMPOSITE_SEPARATOR) > 0 ) ;
}

/*
 *
 */
bool ActiveMQDestination::isWildcard()
{
    if( physicalName != NULL )
    {
        return ( physicalName->find( DestinationFilter::ANY_CHILD ) >= 0 ||
                 physicalName->find( DestinationFilter::ANY_DESCENDENT ) >= 0 ) ;
    }
    return false ;
}

/*
 *
 */
p<string> ActiveMQDestination::toString()
{
    return physicalName ;
}

// --- Static methods ---------------------------------------------

/*
 * A helper method to return a descriptive string for the topic or queue.
 */
p<string> ActiveMQDestination::inspect(p<ActiveMQDestination> destination)
{
    p<string> description = new string() ;

    if( typeid(*destination) == typeid(ITopic) )
        description->assign("Topic(") ;
    else
        description->assign("Queue(") ;
    
    description->append( destination->toString()->c_str() ) ;
    description->append(")") ;

    return description ;
}

/*
 *
 */
/*p<ActiveMQDestination> ActiveMQDestination::transform(p<IDestination> destination)
{
    p<ActiveMQDestination> result = NULL ;

    if( destination != NULL )
    {
        if( typeid(*destination) == typeid(ActiveMQDestination) )
            result = p_cast<ActiveMQDestination> (destination) ;

        else
        {
            if( typeid(ITopic).before(typeid(IDestination)) )
                result = new ActiveMQTopic( (p_cast<ITopic> (destination))->getTopicName()->c_str() ) ;

            else if( typeid(*destination).before(typeid(IQueue)) )
                result = new ActiveMQQueue( (p_cast<IQueue> (destination))->getQueueName()->c_str() ) ;

            else if( typeid(ITemporaryQueue).before(typeid(*destination)) )
                result = new ActiveMQTempQueue( (p_cast<IQueue> (destination))->getQueueName()->c_str() ) ;

            else if( typeid(ITemporaryTopic).before(typeid(*destination)) )
                result = new ActiveMQTempTopic( (p_cast<ITopic> (destination))->getTopicName()->c_str() ) ;

        } 
    }
    return result ;
}*/

/*
 *
 */
p<ActiveMQDestination> ActiveMQDestination::createDestination(int type, const char* physicalName)
{
    p<ActiveMQDestination> result = NULL ;

    if( type == ActiveMQDestination::ACTIVEMQ_TOPIC )
        result = new ActiveMQTopic(physicalName) ;

    else if( type == ActiveMQDestination::ACTIVEMQ_TEMPORARY_TOPIC )
        result = new ActiveMQTempTopic(physicalName) ;

    else if (type == ActiveMQDestination::ACTIVEMQ_QUEUE)
        result = new ActiveMQQueue(physicalName) ;

    else
        result = new ActiveMQTempQueue(physicalName) ;

    return result ;
}

/*
 *
 */
p<string> ActiveMQDestination::createTemporaryName(const char* clientId)
{
    p<string> tempName = new string() ;

    tempName->assign( ActiveMQDestination::TEMP_PREFIX ) ;
    tempName->append(clientId) ;
    tempName->append( ActiveMQDestination::TEMP_POSTFIX ) ;

    return tempName ;
}

/*
 *
 */
p<string> ActiveMQDestination::getClientId(p<ActiveMQDestination> destination)
{
    p<string> answer = NULL ;

    if( destination != NULL && destination->isTemporary() )
    {
        p<string> name = destination->getPhysicalName() ;
        int      start = (int)name->find(TEMP_PREFIX),
                 stop ;

        if( start >= 0 )
        {
            start += (int)strlen(TEMP_PREFIX) ;
            stop   = (int)name->find_last_of(TEMP_POSTFIX) ;

            if( stop > start && stop < (int)name->length() )
                answer->assign( name->substr(start, stop) ) ;
        } 
    }
    return answer; 
}

/*
 *
 */
int ActiveMQDestination::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw (IOException)
{
    int size = 0 ;

    size += marshaller->marshalString(physicalName, mode, ostream) ; 
    return size ;
}

/*
 *
 */
void ActiveMQDestination::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw (IOException)
{
    physicalName = p_cast<string>(marshaller->unmarshalString(mode, istream)) ; 
}
