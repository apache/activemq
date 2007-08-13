"""
Module Message
"""

class Message(object):
    """
    Objects of this class represent ActiveMQ messages.
    They are used to store the 'travel path' of every message.
    This class also stores a collection of all the Message objects.
    """
    
    messages = {}
    messageCount = 0
        
    def __init__(self, producer, prodSeqId, advisory):
        """
        Constructs a message object, given a producer and producer sequence id.
        """
        
        self.producer = producer
        self.prodSeqId = prodSeqId
        self.advisory = advisory
        self.sendingConnections = {}
        self.receivingConnections = {}
        
        Message.messageCount += 1
    
    @classmethod
    def clearData(cls):
        """
        Deletes all the messages.
        Returns nothing.
        """
        
        cls.messages.clear()
        cls.messageCount = 0
    
    @classmethod
    def getMessage(cls, producer, prodSeqId, advisory = False):
        """
        Returns the Message object identified by (producer, prodSeqId)
        where producer is a producer object and prodSeqId is a producer sequence id
        message was first sent.
        
        If the Message object does not exist, it will be created.
        Returns a Message object.
        """
        
        messageId = (producer, prodSeqId)
        
        if messageId not in cls.messages:
            cls.messages[messageId] = Message(producer, prodSeqId, advisory)
        
        return cls.messages[messageId]
    
    @classmethod
    def exists(cls, producer, prodSeqId):
        """
        Returns if there is a Message object identified by (producer, prodSeqId)
        """
        
        return (producer, prodSeqId) in cls.messages
        
    def addSendingConnection(self, connection, direction, mostRecentConId, commandId, timestamp):
        """
        Adds a connection to the set of connections through which this message was sent.
        The 'direction' argument is True if the message was sent from the file
        connection.fromFile to the file connection.toFile, and False otherwise.
        'timestamp' is a string with the moment this message was sent trough the connection.
        
        Returns nothing.
        """
        
        storedConnection = (connection, direction)
        if storedConnection in self.sendingConnections:
            self.sendingConnections[storedConnection].append((mostRecentConId, commandId, timestamp))
        else:
            self.sendingConnections[storedConnection] = [(mostRecentConId, commandId, timestamp)]
        
    def addReceivingConnection(self, connection, direction, mostRecentConId, commandId, timestamp):
        """
        Adds a connection to the set of connections where this message was received.
        The 'direction' argument is True if the message was sent from the file
        connection.fromFile to the file connection.toFile, and False otherwise.
        'timestamp' is a string with the moment this message was received trough the connection.
        
        Returns nothing.
        """
        
        storedConnection = (connection, direction)
        if storedConnection in self.receivingConnections:
            self.receivingConnections[storedConnection].append((mostRecentConId, commandId, timestamp))
        else:
            self.receivingConnections[storedConnection] = [(mostRecentConId, commandId, timestamp)]

    def getFiles(self):
        """
        Returns a 2-tuple with the following 2 sets:
            -set of LogFile objects where this message was sent.
            -set of LogFile objects where this message was received.
        """
        
        sendingFiles = set() 
        receivingFiles = set()
        
        for connection, direction in self.sendingConnections:
            
            if direction:
                sendingFiles.add(connection.fromFile)
            else:
                sendingFiles.add(connection.toFile)

        for connection, direction in self.receivingConnections:
                        
            if direction:
                receivingFiles.add(connection.toFile)
            else:
                receivingFiles.add(connection.fromFile)
            
        return sendingFiles, receivingFiles