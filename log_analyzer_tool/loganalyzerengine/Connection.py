"""
Module Connection
"""
import itertools

class Connection(object):
    """
    This class represents an ActiveMQ Connection.
    It also stores a collection of the connections
    that have been read in the log files.
    
    A Connection's id is the ActiveMQConnection's id. Since this is usually a long
    alphanumerical string, it is called 'longId'.
    Each new Connection gets also assigned an integer 'shortId' automatically.
    The function of this 'shortId' is to make understanding of displayed data easier.
    
    A Connection has 2 LogFile members, who represent:
    -the log file of the JVM that initiates a connection.
    -the log file of the JVM that receives a connection request.
    
    The purpose of every Connection is to store the following data:
        
        -messages sent through this connection, as a dictionary where the
        key is a tuple (message, direction) and the value is
        a list of timestamps. If the message was sent only one time (normal case),
        the timestamp list will have 1 item only.
        
        -messages received through this connection, as a dictionary
        analogous to the previous one.
        
        -messages sent but not received through this connection, as a list of
        tuples (storedMessage, ntimes, timestamps).
        'storedMessage' is a (message, direction) tuple
        ntimes, an integer, is the number of times a message was sent but not received
        timestamps is a list of timestamps of when the message was sent or received.
        For a message to be in this list, ntimes must be >= 1.
        
        -messages received but not sent through this connection.
        Analog to previous point.
        
        -messages sent more than 2 more times through this connection, as a list of
        tuples (storedMessage, ntimes, timestamps).
        'storedMessage' is a (message, direction) tuple
        ntimes, an integer, is the number of times a message was sent.
        timestamps is a list of timestamps of when the message was sent.
        For a message to be in this list, ntimes must be >= 2.
        
        -messages received more than 2 more times through this connection.
        Identical structure to the previous point.
    
    The 'direction' value is either True or False.
    True represents that the message was sent from the JVM writing to the
    'from' file, to the JVM writing to the 'to' file.
    False represents the opposite.
    """

    #dictionary whose keys are connection ids, and whose values
    #are Connection objects
    connections = {}
    nConnections = 0
    connectionIdList = []
    
    def __init__(self, longId, fromFile = None, toFile = None):
        """
        Constructs a Connection object.
        longId : string
        fromFile: LogFile object
        to: LogFile object
        The ActiveMQConnection's id has to be provided.
        Optionally, 2 LogFile objects can be provided.
        The 'from' file is the log file of the JVM that initiates a connection.
        The 'to' file is the log file of the JVM that receives a connection request.
        
        A new connection gets automatically a new 'shortId', which is an integer.
        The longId gets also stored in a list of longIds.
        
        Returns a Connection object.
        """
        
        self.longId = longId
        self.fromFile = fromFile
        self.toFile = toFile
        
        self.shortId = Connection.nConnections
        Connection.connectionIdList.append(longId)
        Connection.nConnections += 1

        self.producers = set()
        self.consumers = set()

        self.sent = {}
        self.received = {}

        self.duplicateSent = None
        self.duplicateReceived = None
        self.sentButNotReceived = None
        self.receivedButNotSent = None
        
        self.calculated = False

    @classmethod
    def clearData(cls):
        """
        Deletes all information read about connections.
        
        Returns nothing.
        """
        
        cls.connections.clear()
        cls.nConnections = 0
        del cls.connectionIdList[:]
        
    @classmethod
    def getConnectionByLongId(cls, longId):
        """
        Retrieves the connection whose id is 'longId'.
        If there is no connection with this id, a new
        one is created with this id.
        
        Returns a Connection object.
        """
        
        if longId not in cls.connections:
            cls.connections[longId] = Connection(longId)

        return cls.connections[longId]
    
    @classmethod
    def getConnectionByShortId(cls, shortId):
        """
        Retrieves the connection whose shortId is 'shortId'.
        If there is no connection with this id, 
        an IndexError exception will be thrown.
        
        Returns a Connection object.
        Throws an IndexError if the short id does not exist.
        """

        return cls.connections[cls.connectionIdList[shortId]]
    
    @classmethod
    def shortIdToLongId(cls, shortId):
        """
        Transforms a connection's short id to a long id.
        Returns the long id.
        Throws an IndexError if the short id does not exist.
        """
        return cls.connectionIdList[shortId]
    
    @classmethod
    def longIdToShortId(cls, longId):
        """
        Transforms a connection's long id to a short id.
        Returns the short id.
        Throws an KeyError if the short id does not exist.
        """
        try:
            return cls.connections[longId].shortId
        except KeyError:
            print longId
            print cls.connections
            raise

    @classmethod
    def setFrom(cls, longId, fromFile):
        """
        Sets the 'from' LogFile object for the connection whose id is 'longId'.
        The 'from' file is the log file of the JVM that initiates a connection.
        If there is not yet a connection whose id is 'longId', a new one is
        created with this longId and this 'from' file.
        
        Returns nothing.
        """
        
        if longId not in cls.connections:
            cls.connections[longId] = Connection(longId, fromFile = fromFile)
        else:
            cls.connections[longId].fromFile = fromFile

    @classmethod
    def setTo(cls, longId, toFile):
        """
        Sets the 'to' LogFile object for the connection whose id is 'longId'.
        The 'to' file is the log file of the JVM that receives a connection request.
        If there is not yet a connection whose id is 'longId', a new one is
        created with this longId and this 'to' file.
        
        Returns nothing.
        """
        
        if longId not in cls.connections:
            cls.connections[longId] = Connection(longId, toFile = toFile)
        else:
            cls.connections[longId].toFile = toFile



    @classmethod
    def exists(cls, longId):
        """
        Returns if there is a connection whose id is 'longId'
        """
        
        return longId in cls.connections
        


    def addProducer(self, producer):
        """
        Adds a producer to the set of this connection's producers.
        Returns nothing.
        """
        self.producers.add(producer)
        
    def addConsumer(self, consumer):
        """
        Adds a consumer to the set of this connection's consumers.
        Returns nothing.
        """
        self.consumers.add(consumer)
        
    def addSentMessage(self, message, direction, timestamp):
        """
        Adds a message to the set of messages sent through this connection.
            message: a Message object
            direction: True if this message was sent from self.fromFile to self.to
                       False if this message was sent from self.toFile to self.fromFile
            timestamp: a string with the time this message was sent
                       
        If the message has already been sent in this direction, it gets added to the
        collection of duplicate sent messages.
        
        Returns nothing.
        """
        
        storedMessage = (message, direction)
        
        if storedMessage in self.sent:
            self.sent[storedMessage].append(timestamp)
        else:
            self.sent[storedMessage] = [timestamp]
            
    def addReceivedMessage(self, message, direction, timestamp):
        """
        Adds a message to the set of messages received through this connection.
            message: a message object
            direction: True if this message was sent from self.fromFile to self.to
                       False if this message was sent from self.toFile to self.fromFile
            timestamp: a string with the time this message was sent
                       
        If the message has already been received in this direction, it gets added to the
        collection of duplicate received messages.
        
        Returns nothing.
        """
        
        storedMessage = (message, direction)
        
        if storedMessage in self.received:
            self.received[storedMessage].append(timestamp)
        else:
            self.received[storedMessage] = [timestamp]
            
    def getErrors(self):
        """
        Processes the data previously gathered to find incorrect situations.
        
        Returns a 4-tuple with:
            -collection of sent but not received messages, through this Connection.
            This collection is a list of (storedMessage, ntimes, timestamps) tuples where:
               *'storedMessage' is a (message, direction) tuple.
               *'ntimes' is an integer, representing how many times the message was sent but not received.
               *'timestamps' is a list of strings with the timestamps when this message was sent / received.
               
            -collection of received but not sent messages, through this Connection.
            This collection is a list of (storedMessage, ntimes, timestamps) tuples where:
               *'storedMessage' is a (message, direction) tuple.
               *'ntimes' is an integer, representing how many times the message was received but not sent.
               *'timestamps' is a list of strings with the timestamps when this message was sent / received.
                              
            -collection of duplicate sent messages, through this Connection.
            This collection is a list of (message, timestamps) tuples where:
               *'storedMessage' is a (shortId, commandId, direction) tuple.
               *'ntimes' is an integer, representing how many times the message sent.
               *'timestamps' is a list of strings with the timestamps when this message was sent.
               
            -collection of duplicate received messages, through this Connection.
            This collection is a list of (message, timestamps) tuples where:
               *'storedMessage' is a (message, direction) tuple.
               *'ntimes' is an integer, representing how many times the message received.
               *'timestamps' is a list of strings with the timestamps when this message was received.
               
        The data is only calculated once, and then successive calls of this method return always
        the same erros unles self.calculated is set to False.
        """
        
        if not self.calculated:
            self.sentButNotReceived = []
            for message, timestamps in self.sent.iteritems():
                if message not in self.received:
                    self.sentButNotReceived.append((message, len(timestamps), timestamps))
                else:
                    difference = len(timestamps) - len(self.received[message])
                    if difference > 0:
                        self.sentButNotReceived.append((message, difference,
                                                        itertools.chain(timestamps, self.received[message])))
            
            self.receivedButNotSent = []
            for message, timestamps in self.received.iteritems():
                if message not in self.sent:
                    self.receivedButNotSent.append((message, len(timestamps), timestamps))
                else:
                    difference = len(timestamps) - len(self.sent[message])
                    if difference > 0:
                        self.receivedButNotSent.append((message, difference,
                                                        itertools.chain(timestamps, self.sent[message])))
                        
            self.duplicateSent = [(message, len(timestamps), timestamps)
                                  for message, timestamps in self.sent.iteritems() if len(timestamps) > 1]
            self.duplicateReceived  = [(message, len(timestamps), timestamps)
                                       for message, timestamps in self.received.iteritems() if len(timestamps) > 1]
            
            self.sentButNotReceived.sort(key = lambda message: (message[0][0].producer.shortId, message[0][0].prodSeqId))
            self.receivedButNotSent.sort(key = lambda message: (message[0][0].producer.shortId, message[0][0].prodSeqId))
            self.duplicateSent.sort(key = lambda message: (message[0][0].producer.shortId, message[0][0].prodSeqId))
            self.duplicateReceived.sort(key = lambda message: (message[0][0].producer.shortId, message[0][0].prodSeqId))
            
            self.calculated = True
        
        return self.sentButNotReceived, self.receivedButNotSent, self.duplicateSent, self.duplicateReceived      

    def __str__(self):
        """
        Represents this Connection object as a string.
        """
        
        return ''.join([self.longId, ' from:', str(self.fromFile), ' to:', str(self.toFile)])