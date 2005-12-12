using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for ActiveMQDestination.
	/// </summary>
	public abstract class ActiveMQDestination {

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
    private String  orderedTarget = DEFAULT_ORDERED_TARGET;
    
    /**
     * The Default Constructor
     */
    protected ActiveMQDestination() {
    }

    /**
     * Construct the ActiveMQDestination with a defined physical name;
     *
     * @param name
     */

    protected ActiveMQDestination(String name) {
        this.physicalName = name;
        this.advisory = name != null && name.startsWith(ADVISORY_PREFIX);
    }


    
    /**
     * @return Returns the advisory.
     */
    public bool isAdvisory() {
        return advisory;
    }
    /**
     * @param advisory The advisory to set.
     */
    public void setAdvisory(bool advisory) {
        this.advisory = advisory;
    }
    
    /**
     * @return true if this is a destination for Consumer advisories
     */
    public bool isConsumerAdvisory(){
        return isAdvisory() && physicalName.startsWith(ActiveMQDestination.CONSUMER_ADVISORY_PREFIX);
    }
    
    /**
     * @return true if this is a destination for Producer advisories
     */
    public bool isProducerAdvisory(){
        return isAdvisory() && physicalName.startsWith(ActiveMQDestination.PRODUCER_ADVISORY_PREFIX);
    }
    
    /**
     * @return true if this is a destination for Connection advisories
     */
    public bool isConnectionAdvisory(){
        return isAdvisory() && physicalName.startsWith(ActiveMQDestination.CONNECTION_ADVISORY_PREFIX);
    }
    
    /**
     * @return Returns the exclusive.
     */
    public bool isExclusive() {
        return exclusive;
    }
    /**
     * @param exclusive The exclusive to set.
     */
    public void setExclusive(bool exclusive) {
        this.exclusive = exclusive;
    }
    /**
     * @return Returns the ordered.
     */
    public bool isOrdered() {
        return ordered;
    }
    /**
     * @param ordered The ordered to set.
     */
    public void setOrdered(bool ordered) {
        this.ordered = ordered;
    }
    /**
     * @return Returns the orderedTarget.
     */
    public String getOrderedTarget() {
        return orderedTarget;
    }
    /**
     * @param orderedTarget The orderedTarget to set.
     */
    public void setOrderedTarget(String orderedTarget) {
        this.orderedTarget = orderedTarget;
    }
    /**
     * A helper method to return a descriptive string for the topic or queue
     * @param destination
     *
     * @return a descriptive string for this queue or topic
     */
    public static String inspect(ActiveMQDestination destination) {
        if (destination is Topic) {
            return "Topic(" + destination.toString() + ")";
        }
        else {
            return "Queue(" + destination.toString() + ")";
        }
    }

    /**
     * @param destination
     * @return @throws JMSException
     * @throws javax.jms.JMSException
     */
    public static ActiveMQDestination transformDestination(ActiveMQDestination destination) {
        ActiveMQDestination result = null;
        if (destination != null) {
            if (destination is ActiveMQDestination) {
                result = (ActiveMQDestination) destination;
            }
            else {
                if (destination is TemporaryQueue) {
                    result = new ActiveMQTemporaryQueue(((Queue) destination).getQueueName());
                }
                else if (destination is TemporaryTopic) {
                    result = new ActiveMQTemporaryTopic(((Topic) destination).getTopicName());
                }
                else if (destination is Queue) {
                    result = new ActiveMQTemporaryQueue(((Queue) destination).getQueueName());
                }
                else if (destination is Topic) {
                    result = new ActiveMQTemporaryTopic(((Topic) destination).getTopicName());
                }
            }
        }
        return result;
    }

    /**
     * Write an ActiveMQDestination to a Stream
     *
     * @param destination
     * @param dataOut
     * @throws IOException
     */

    public static void writeToStream(ActiveMQDestination destination, Object dataOut)  {
        //TODO SERILIZATION
    }

    /**
     * Read an ActiveMQDestination  from a Stream
     *
     * @param dataIn
     * @return the ActiveMQDestination
     * @throws IOException
     */

    public static ActiveMQDestination readFromStream(Object dataIn)  {
		//TODO Serilization
    }
    
    /**
     * Create a Destination
     * @param type
     * @param pyhsicalName
     * @return
     */
    public static ActiveMQDestination createDestination(int type,String pyhsicalName){
        ActiveMQDestination result = null;
        if (type == ACTIVEMQ_TOPIC) {
            result = new ActiveMQTopic(pyhsicalName);
        }
        else if (type == ACTIVEMQ_TEMPORARY_TOPIC) {
            result = new ActiveMQTemporaryTopic(pyhsicalName);
        }
        else if (type == ACTIVEMQ_QUEUE) {
            result = new ActiveMQQueue(pyhsicalName);
        }
        else {
            result = new ActiveMQTemporaryQueue(pyhsicalName);
        }
        return result;
    }

    /**
     * Create a temporary name from the clientId
     *
     * @param clientId
     * @return
     */
    public static String createTemporaryName(String clientId) {
        return TEMP_PREFIX + clientId + TEMP_POSTFIX;
    }

    /**
     * From a temporary destination find the clientId of the Connection that created it
     *
     * @param destination
     * @return the clientId or null if not a temporary destination
     */
    public static String getClientId(ActiveMQDestination destination) {
        String answer = null;
        if (destination != null && destination.isTemporary()) {
            String name = destination.getPhysicalName();
            int start = name.indexOf(TEMP_PREFIX);
            if (start >= 0) {
                start += TEMP_PREFIX.length();
                int stop = name.lastIndexOf(TEMP_POSTFIX);
                if (stop > start && stop < name.length()) {
                    answer = name.substring(start, stop);
                }
            }
        }
        return answer;
    }


    /**
     * @param o object to compare
     * @return 1 if this > o else 0 if they are equal or -1 if this < o
     */
    public int compareTo(Object o) {
        if (o is ActiveMQDestination) {
            return compareTo((ActiveMQDestination) o);
        }
        return -1;
    }

    /**
     * Lets sort by name first then lets sort topics greater than queues
     *
     * @param that another destination to compare against
     * @return 1 if this > that else 0 if they are equal or -1 if this < that
     */
    public int compareTo(ActiveMQDestination that) {
        int answer = 0;
        if (physicalName != that.physicalName) {
            if (physicalName == null) {
                return -1;
            }
            else if (that.physicalName == null) {
                return 1;
            }
            answer = physicalName.compareTo(that.physicalName);
        }
        if (answer == 0) {
            if (isTopic()) {
                if (that.isQueue()) {
                    return 1;
                }
            }
            else {
                if (that.isTopic()) {
                    return -1;
                }
            }
        }
        return answer;
    }


    /**
     * @return Returns the Destination type
     */

    public abstract int getDestinationType();


    /**
     * @return Returns the physicalName.
     */
    public String getPhysicalName() {
        return this.physicalName;
    }

    /**
     * @param newPhysicalName The physicalName to set.
     */
    public void setPhysicalName(String newPhysicalName) {
        this.physicalName = newPhysicalName;
    }

    /**
     * Returns true if a temporary Destination
     *
     * @return true/false
     */

    public bool isTemporary() {
        return getDestinationType() == ACTIVEMQ_TEMPORARY_TOPIC ||
                getDestinationType() == ACTIVEMQ_TEMPORARY_QUEUE;
    }

    /**
     * Returns true if a Topic Destination
     *
     * @return true/false
     */

    public bool isTopic() {
        return getDestinationType() == ACTIVEMQ_TOPIC ||
                getDestinationType() == ACTIVEMQ_TEMPORARY_TOPIC;
    }

    /**
     * Returns true if a Queue Destination
     *
     * @return true/false
     */
    public bool isQueue() {
        return !isTopic();
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
    public bool isComposite() {
        return physicalName.indexOf(COMPOSITE_SEPARATOR) > 0;
    }

    /**
     * Returns a list of child destinations if this destination represents a composite
     * destination.
     *
     * @return
     */
    /*public List getChildDestinations() {
        List answer = new ArrayList();
        StringTokenizer iter = new StringTokenizer(physicalName, COMPOSITE_SEPARATOR);
        while (iter.hasMoreTokens()) {
            String name = iter.nextToken();
            Destination child = null;
            if (name.startsWith(QUEUE_PREFIX)) {
                child = new ActiveMQQueue(name.substring(QUEUE_PREFIX.length()));
            }
            else if (name.startsWith(TOPIC_PREFIX)) {
                child = new ActiveMQTopic(name.substring(TOPIC_PREFIX.length()));
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

    public String toString() {
        return this.physicalName;
    }

    /**
     * @return hashCode for this instance
     */

    public int hashCode() {
        int answer = 0xcafebabe;

        if (this.physicalName != null) {
            answer = physicalName.hashCode();
        }
        if (isTopic()) {
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

    public bool equals(Object obj) {
        bool result = this == obj;
        if (!result && obj != null && obj is ActiveMQDestination) {
            ActiveMQDestination other = (ActiveMQDestination) obj;
            result = this.getDestinationType() == other.getDestinationType() &&
                    this.physicalName.equals(other.physicalName);
        }
        return result;
    }


    /**
     * @return true if the destination matches multiple possible destinations
     */
    public bool isWildcard() {
        if (physicalName != null) {
            return physicalName.indexOf(DestinationFilter.ANY_CHILD) >= 0
                    || physicalName.indexOf(DestinationFilter.ANY_DESCENDENT) >= 0;
        }
        return false;
    }

    /**
     * @param destination
     * @return  true if the given destination matches this destination; including wildcards
     */
    public bool matches(ActiveMQDestination destination) {
        if (isWildcard()) {
            return getDestinationFilter().matches(destination);
        }
        else {
            return equals(destination);
        }
    }
    

    /**
     * @return the DestinationFilter
     */
    public DestinationFilter getDestinationFilter() {
        if (filter == null) {
            filter = DestinationFilter.parseFilter(this);
        }
        return filter;
    }

    /**
     * @return the associated paths associated with this Destination
     */
    public String[] getDestinationPaths() {
        if (paths == null) {
            paths = DestinationPath.getDestinationPaths(physicalName);
        }
        return paths;
    }





    
    
    // Implementation methods
    //-------------------------------------------------------------------------


    /**
     * Factory method to create a child destination if this destination is a composite
     * @param name
     * @return the created Destination
     */
    public abstract ActiveMQDestination createDestination(String name);

    
}

}
