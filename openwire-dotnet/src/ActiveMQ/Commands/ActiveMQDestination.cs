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
using ActiveMQ;
using ActiveMQ.Commands;
using ActiveMQ.OpenWire;
using JMS;
using System;



/// <summary>
/// Summary description for ActiveMQDestination.
/// </summary>
namespace ActiveMQ.Commands
{
	public abstract class ActiveMQDestination : AbstractCommand, IDestination
    {
        
        /**
         * Topic Destination object
         */
        public const int ACTIVEMQ_TOPIC = 1;
        /**
         * Temporary Topic Destination object
         */
        public const int ACTIVEMQ_TEMPORARY_TOPIC = 2;
        
        /**
         * Queue Destination object
         */
        public const int ACTIVEMQ_QUEUE = 3;
        /**
         * Temporary Queue Destination object
         */
        public const int ACTIVEMQ_TEMPORARY_QUEUE = 4;
        
        /**
         * prefix for Advisory message destinations
         */
        public const String ADVISORY_PREFIX = "ActiveMQ.Advisory.";
        
        /**
         * prefix for consumer advisory destinations
         */
        public const String CONSUMER_ADVISORY_PREFIX = ADVISORY_PREFIX + "Consumers.";
        
        /**
         * prefix for producer advisory destinations
         */
        public const String PRODUCER_ADVISORY_PREFIX = ADVISORY_PREFIX + "Producers.";
        
        /**
         * prefix for connection advisory destinations
         */
        public const String CONNECTION_ADVISORY_PREFIX = ADVISORY_PREFIX + "Connections.";
        
        /**
         * The default target for ordered destinations
         */
        public const String DEFAULT_ORDERED_TARGET = "coordinator";
        
        private const int NULL_DESTINATION = 10;
        
        private const String TEMP_PREFIX = "{TD{";
        private const String TEMP_POSTFIX = "}TD}";
        private const String COMPOSITE_SEPARATOR = ",";
        private const String QUEUE_PREFIX = "queue://";
        private const String TOPIC_PREFIX = "topic://";
        
        
        private String physicalName = "";
        
        // Cached transient data
        private bool exclusive;
        private bool ordered;
        private bool advisory;
        private String orderedTarget = DEFAULT_ORDERED_TARGET;
        
        
        /**
         * The Default Constructor
         */
        protected ActiveMQDestination()
        {
        }
        
        /**
         * Construct the Destination with a defined physical name;
         *
         * @param name
         */
        protected ActiveMQDestination(String name)
        {
            this.physicalName = name;
            this.advisory = name != null && name.StartsWith(ADVISORY_PREFIX);
        }
        
        
        
        /**
         * @return Returns the advisory.
         */
        public bool IsAdvisory()
        {
            return advisory;
        }
        /**
         * @param advisory The advisory to set.
         */
        public void SetAdvisory(bool advisory)
        {
            this.advisory = advisory;
        }
        
        /**
         * @return true if this is a destination for Consumer advisories
         */
        public bool IsConsumerAdvisory()
        {
            return IsAdvisory() && physicalName.StartsWith(CONSUMER_ADVISORY_PREFIX);
        }
        
        /**
         * @return true if this is a destination for Producer advisories
         */
        public bool IsProducerAdvisory()
        {
            return IsAdvisory() && physicalName.StartsWith(PRODUCER_ADVISORY_PREFIX);
        }
        
        /**
         * @return true if this is a destination for Connection advisories
         */
        public bool IsConnectionAdvisory()
        {
            return IsAdvisory() && physicalName.StartsWith(CONNECTION_ADVISORY_PREFIX);
        }
        
        /**
         * @return Returns the exclusive.
         */
        public bool IsExclusive()
        {
            return exclusive;
        }
        /**
         * @param exclusive The exclusive to set.
         */
        public void SetExclusive(bool exclusive)
        {
            this.exclusive = exclusive;
        }
        /**
         * @return Returns the ordered.
         */
        public bool IsOrdered()
        {
            return ordered;
        }
        /**
         * @param ordered The ordered to set.
         */
        public void SetOrdered(bool ordered)
        {
            this.ordered = ordered;
        }
        /**
         * @return Returns the orderedTarget.
         */
        public String GetOrderedTarget()
        {
            return orderedTarget;
        }
        /**
         * @param orderedTarget The orderedTarget to set.
         */
        public void SetOrderedTarget(String orderedTarget)
        {
            this.orderedTarget = orderedTarget;
        }
        /**
         * A helper method to return a descriptive string for the topic or queue
         * @param destination
         *
         * @return a descriptive string for this queue or topic
         */
        public static String Inspect(ActiveMQDestination destination)
        {
            if (destination is ITopic)
            {
                return "Topic(" + destination.ToString() + ")";
            }
            else
            {
                return "Queue(" + destination.ToString() + ")";
            }
        }
        
        /**
         * @param destination
         * @return @throws JMSException
         * @throws javax.jms.JMSException
         */
        public static ActiveMQDestination Transform(IDestination destination)
        {
            ActiveMQDestination result = null;
            if (destination != null)
            {
                if (destination is ActiveMQDestination)
                {
                    result = (ActiveMQDestination) destination;
                }
                else
                {
                    if (destination is ITemporaryQueue)
                    {
                        result = new ActiveMQTempQueue(((IQueue) destination).QueueName);
                    }
                    else if (destination is ITemporaryTopic)
                    {
                        result = new ActiveMQTempTopic(((ITopic) destination).TopicName);
                    }
                    else if (destination is IQueue)
                    {
                        result = new ActiveMQQueue(((IQueue) destination).QueueName);
                    }
                    else if (destination is ITopic)
                    {
                        result = new ActiveMQTopic(((ITopic) destination).TopicName);
                    }
                }
            }
            return result;
        }
        
        /**
         * Create a Destination
         * @param type
         * @param pyhsicalName
         * @return
         */
        public static ActiveMQDestination CreateDestination(int type, String pyhsicalName)
        {
            ActiveMQDestination result = null;
            if (type == ACTIVEMQ_TOPIC)
            {
                result = new ActiveMQTopic(pyhsicalName);
            }
            else if (type == ACTIVEMQ_TEMPORARY_TOPIC)
            {
                result = new ActiveMQTempTopic(pyhsicalName);
            }
            else if (type == ACTIVEMQ_QUEUE)
            {
                result = new ActiveMQQueue(pyhsicalName);
            }
            else
            {
                result = new ActiveMQTempQueue(pyhsicalName);
            }
            return result;
        }
        
        /**
         * Create a temporary name from the clientId
         *
         * @param clientId
         * @return
         */
        public static String CreateTemporaryName(String clientId)
        {
            return TEMP_PREFIX + clientId + TEMP_POSTFIX;
        }
        
        /**
         * From a temporary destination find the clientId of the Connection that created it
         *
         * @param destination
         * @return the clientId or null if not a temporary destination
         */
        public static String GetClientId(ActiveMQDestination destination)
        {
            String answer = null;
            if (destination != null && destination.IsTemporary())
            {
                String name = destination.PhysicalName;
                int start = name.IndexOf(TEMP_PREFIX);
                if (start >= 0)
                {
                    start += TEMP_PREFIX.Length;
                    int stop = name.LastIndexOf(TEMP_POSTFIX);
                    if (stop > start && stop < name.Length)
                    {
                        answer = name.Substring(start, stop);
                    }
                }
            }
            return answer;
        }
        
        
        /**
         * @param o object to compare
         * @return 1 if this is less than o else 0 if they are equal or -1 if this is less than o
         */
        public int CompareTo(Object o)
        {
            if (o is ActiveMQDestination)
            {
                return CompareTo((ActiveMQDestination) o);
            }
            return -1;
        }
        
        /**
         * Lets sort by name first then lets sort topics greater than queues
         *
         * @param that another destination to compare against
         * @return 1 if this is less than o else 0 if they are equal or -1 if this is less than o
         */
        public int CompareTo(ActiveMQDestination that)
        {
            int answer = 0;
            if (physicalName != that.physicalName)
            {
                if (physicalName == null)
                {
                    return -1;
                }
                else if (that.physicalName == null)
                {
                    return 1;
                }
                answer = physicalName.CompareTo(that.physicalName);
            }
            if (answer == 0)
            {
                if (IsTopic())
                {
                    if (that.IsQueue())
                    {
                        return 1;
                    }
                }
                else
                {
                    if (that.IsTopic())
                    {
                        return -1;
                    }
                }
            }
            return answer;
        }
        
        
        /**
         * @return Returns the Destination type
         */
        
        public abstract int GetDestinationType();
        
        
        public String PhysicalName
        {
            get { return this.physicalName; }
            set { this.physicalName = value; }
        }
        
        /**
         * Returns true if a temporary Destination
         *
         * @return true/false
         */
        
        public bool IsTemporary()
        {
            return GetDestinationType() == ACTIVEMQ_TEMPORARY_TOPIC
                || GetDestinationType() == ACTIVEMQ_TEMPORARY_QUEUE;
        }
        
        /**
         * Returns true if a Topic Destination
         *
         * @return true/false
         */
        
        public bool IsTopic()
        {
            return GetDestinationType() == ACTIVEMQ_TOPIC
                || GetDestinationType() == ACTIVEMQ_TEMPORARY_TOPIC;
        }
        
        /**
         * Returns true if a Queue Destination
         *
         * @return true/false
         */
        public bool IsQueue()
        {
            return !IsTopic();
        }
        
        /**
         * Returns true if this destination represents a collection of
         * destinations; allowing a set of destinations to be published to or subscribed
         * from in one JMS operation.
         * <p/>
         * If this destination is a composite then you can call {@link #getChildDestinations()}
         * to return the list of child destinations.
         *
         * @return true if this destination represents a collection of child destinations.
         */
        public bool IsComposite()
        {
            return physicalName.IndexOf(COMPOSITE_SEPARATOR) > 0;
        }
        
        /*
         * Returns a list of child destinations if this destination represents a composite
         * destination.
         *
         * @return
         */
        /*public List GetChildDestinations() {
         List answer = new ArrayList();
         StringTokenizer iter = new StringTokenizer(physicalName, COMPOSITE_SEPARATOR);
         while (iter.hasMoreTokens()) {
         String name = iter.nextToken();
         Destination child = null;
         if (name.StartsWith(QUEUE_PREFIX)) {
         child = new ActiveMQQueue(name.Substring(QUEUE_PREFIX.Length));
         }
         else if (name.StartsWith(TOPIC_PREFIX)) {
         child = new ActiveMQTopic(name.Substring(TOPIC_PREFIX.Length));
         }
         else {
         child = createDestination(name);
         }
         answer.add(child);
         }
         if (answer.size() == 1) {
         // lets put ourselves inside the collection
         // as we are not really a composite destination
         answer.set(0, this);
         }
         return answer;
         }*/
        
        /**
         * @return string representation of this instance
         */
        
        public override String ToString()
        {
            return this.physicalName;
        }
        
        /**
         * @return hashCode for this instance
         */
        public override int GetHashCode()
        {
            int answer = 37;
            
            if (this.physicalName != null)
            {
                answer = physicalName.GetHashCode();
            }
            if (IsTopic())
            {
                answer ^= 0xfabfab;
            }
            return answer;
        }
        
        /**
         * if the object passed in is equivalent, return true
         *
         * @param obj the object to compare
         * @return true if this instance and obj are equivalent
         */
        public override bool Equals(Object obj)
        {
            bool result = this == obj;
            if (!result && obj != null && obj is ActiveMQDestination)
            {
                ActiveMQDestination other = (ActiveMQDestination) obj;
                result = this.GetDestinationType() == other.GetDestinationType()
                    && this.physicalName.Equals(other.physicalName);
            }
            return result;
        }
        
        
        /**
         * @return true if the destination matches multiple possible destinations
         */
        public bool IsWildcard()
        {
            if (physicalName != null)
            {
                return physicalName.IndexOf(DestinationFilter.ANY_CHILD) >= 0
                    || physicalName.IndexOf(DestinationFilter.ANY_DESCENDENT) >= 0;
            }
            return false;
        }
        
        
        /**
         * Factory method to create a child destination if this destination is a composite
         * @param name
         * @return the created Destination
         */
        public abstract ActiveMQDestination CreateDestination(String name);
    }
}

