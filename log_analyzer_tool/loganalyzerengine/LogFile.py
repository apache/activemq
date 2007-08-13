"""
Module LogFile
"""
import os

class LogFile(object):
    """
    Class that represents an ActiveMQ log file read by the application.
    It also stores a list of all the LogFile objects.
    
    A LogFile object stores the following information:
    -A list of 'outgoing' Connection objects that represent the connections
    created by the JVM that writes this LogFile.
    -A list of 'incoming' Connection objects that represent the connections
    requests received by the JVM that writes this LogFile.
    
    -A dictionary of messages that were sent in this file.
    The keys are Message objects and the values
    are lists of timestamps of when the message was sent.
    -A list of messages that were received in this file.
    The keys are Message objects and the values
    are lists of timestamps of when the message was received.
    
    -A list of messages that were sent in this file more than 1 time (duplicates)
    The list is made of (message, ntimes, timestamps) tuples.
    -A list of messages that were received in this file more than 1 time (duplicates),
    analogous to the previous structure.
    """
    
    logfiles = []
    
    def __init__(self, path):
        """
        Constructs a LogFile object.
        path: a string with the path to the ActiveMQ log file.
        """
        
        self.__path = os.path.abspath(path)
        self.file = open(self.__path, 'r')
        self.outgoing = []
        self.incoming = []
        
        self.sent = {}
        self.received = {}
        
        self.duplicateReceived = None
        self.duplicateSent = None
        
        self.calculated = False
        
        LogFile.logfiles.append(self)
       
    @classmethod
    def clearData(cls):
        """
        Class method erases all the LogFile objects stored in the LogFile class.
        Returns nothing.
        """
        
        del cls.logfiles[:]
    
    @classmethod
    def closeFiles(cls):
        """
        Class method that closes all the LogFile objects stored in the LogFile class.
        Returns nothing.
        """
        
        for logFile in cls.logfiles:
            logFile.file.close()



    def addOutgoingConnection(self, con):
        """
        Adds an 'outgoing' Connection object to this LogFile.
        Returns nothing.
        """
        
        self.outgoing.append(con)

    def addIncomingConnection(self, con):
        """
        Adds an 'incoming' Connection object to this LogFile.
        Returns nothing.
        """
        
        self.incoming.append(con)
        
    def addSentMessage(self, message, timestamp):
        """
        Adds a message to the set of messages that were sent thtough this file.
        If a message gets sent 2 times, it gets added to the set of duplicate sent messages.
        message: a Message object.
        timestamp: a string with the time where this message was sent.
        
        Returns nothing.
        """
        
        if message in self.sent:
            self.sent[message].append(timestamp)
        else:
            self.sent[message] = [timestamp]
        
    def addReceivedMessage(self, message, timestamp):
        """
        Adds a message to the set of messages that were received in this file.
        If a message gets sent 2 times, it gets added to the set of duplicate received messages.
        message: a Message object.
        timestamp: a string with the time where this message was sent.
        
        Returns nothing.
        """
        
        #message = (shortProdId, prodSeqId, False)
        if message in self.received:
            self.received[message].append(timestamp)
        else:
            self.received[message] = [timestamp]
            
    def getErrors(self):
        """
        Returns a 2-tuple with:
            -a list of (message, ntimes, timestamps) tuples, with the duplicate sent messages
            that appear in more than one connection in this file.
            'message' is a Message object.
            'ntimes' is an integer stating how many times the message was sent ( always >= 2)
            'timestamps' is a list of timestamps with the instants the message was sent.
            
            -a list of (message, ntimes, timestamps) tuples, with the duplicate received messages
            that appear in more than one connection in this file.
            Structure analogous to previous one.
        
        The data is only calculated once, and then successive calls of this method return always
        the same erros unles self.calculated is set to False.
        """
        
        if not self.calculated:
            
            duplicateSentTemp = [(message, len(timestamps), timestamps)
                                 for message, timestamps in self.sent.iteritems() if len(timestamps) > 1]
            self.duplicateSent = []
            
            for message, _, timestamps in duplicateSentTemp:
                connections = []
                for connection, direction in message.sendingConnections:
                    if direction and connection.fromFile == self       \
                    or not direction and connection.toFile == self:
                        connections.append(connection)
                if len(connections) > 1:
                    self.duplicateSent.append((message, len(timestamps), timestamps))
                    
            duplicateReceivedTemp = [(message, len(timestamps), timestamps)
                                      for message, timestamps in self.received.iteritems() if len(timestamps) > 1]
            self.duplicateReceived = []
            
            for message, _, timestamps in duplicateReceivedTemp:
                connections = []
                for connection, direction in message.receivingConnections:
                    if direction and connection.toFile == self       \
                    or not direction and connection.fromFile == self:
                        connections.append(connection)
                if len(connections) > 1:
                    self.duplicateReceived.append((message, len(timestamps), timestamps))
                    
            self.duplicateSent.sort(key = lambda message: (message[0].producer.shortId, message[0].prodSeqId))
            self.duplicateReceived.sort(key = lambda message: (message[0].producer.shortId, message[0].prodSeqId))
                    
            self.calculated = True
                  
        return self.duplicateSent, self.duplicateReceived

    def close(self):
        """
        Closes the underlying file.
        Returns nothing.
        """
        
        self.file.close()

    def __str__(self):
        """
        Returns a string representation of this object.
        """
        
        return self.__path
    
