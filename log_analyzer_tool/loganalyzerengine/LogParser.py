"""
Module LogParser
"""
import os, sys, time

from LogFile import LogFile
from Connection import Connection
from Producer import Producer
from Consumer import Consumer
from Message import Message

MESSAGE_TYPES = frozenset(['ActiveMQBytesMessage', 'ActiveMQTextMessage'])
DISPATCH_MESSAGE = 'MessageDispatch'
ADVISORY_TEXT = 'Advisory'
CONSUMER_TEXT = 'toConsumer:'

class LogParser(object):
    """
    This class is in charge of parsing the log files and storing the data
    as Connection, LogFile and Message objects.
    """
    
    instance = None
    
    @classmethod
    def getInstance(cls):
        """
        Returns the sole instance of the class.
        """
        
        if cls.instance is None:
            cls.instance = LogParser()
        return cls.instance
    
    @classmethod
    def deleteInstance(cls):
        """
        Deletes the sole instance of the class
        """
        
        cls.instance = None
    
    def parse (self, logFile):
        """
        Parses the information in a log file.
        logFile should be a LogFile object.
        
        Returns nothing.
        """
        try:
            for line in logFile.file:
                loggedMessage = line.partition('$$ ')[2]
                if loggedMessage != '':
                    spacedStrings = loggedMessage.split()
                    
                    if spacedStrings[1] == 'ConnectionInfo':
                        connectionId = spacedStrings[3]
                        
                        if spacedStrings[0] == 'SENDING:':
                            logFile.addOutgoingConnection(Connection.getConnectionByLongId(connectionId))
                            Connection.setFrom(connectionId, logFile)
        
                        elif spacedStrings[0] == 'RECEIVED:':
                            logFile.addIncomingConnection(Connection.getConnectionByLongId(connectionId))
                            Connection.setTo(connectionId, logFile)
                            
                        else:
                            raise Exception('Exception: ConnectionInfo: not SENDING or RECEIVED')
                    
                    elif spacedStrings[1] in MESSAGE_TYPES or spacedStrings[1] == DISPATCH_MESSAGE:
                        timestamp = line[0:23]
                        commaValues = spacedStrings[3].split(',')
                        
                        messageId = commaValues[0]
                        producerId = messageId[:messageId.rindex(':')]
                        
                        connection = Connection.getConnectionByLongId(commaValues[2]) #commaValues[2] = connectionId
                        producer = Producer.getProducerByLongId(producerId)
                        producerConnection = Connection.getConnectionByLongId(producerId.rsplit(':', 2)[0]) #producerConnectionId
                        message = Message.getMessage(producer,
                                                     int(messageId[messageId.rindex(':') + 1:]), #producerSequenceId
                                                     commaValues[-1] == ADVISORY_TEXT)
                        
                        producerConnection.addProducer(producer)
                        
                        if spacedStrings[1] in MESSAGE_TYPES:
                            
                            if spacedStrings[0] == 'SENDING:':
                                
                                direction = (logFile == connection.fromFile)
                                connection.addSentMessage(message, direction, timestamp)
                                logFile.addSentMessage(message, timestamp)
                                message.addSendingConnection(connection, direction, connection,
                                                             int(commaValues[1]), timestamp) #commaValues[1] = commandId
                                
                            elif spacedStrings[0] == 'RECEIVED:':
                                
                                direction = (logFile == connection.toFile)
                                connection.addReceivedMessage(message, direction, timestamp)
                                logFile.addReceivedMessage(message, timestamp)
                                message.addReceivingConnection(connection, direction, connection,
                                                               int(commaValues[1]), timestamp) #commaValues[1] = commandId
                        
                        elif spacedStrings[1] == DISPATCH_MESSAGE:
                            
                            #additional parsing to get the consumer
                            consumerId = spacedStrings[4][len(CONSUMER_TEXT):]
                            consumer = Consumer.getConsumerByLongId(consumerId)
                            consumerConnection = Connection.getConnectionByLongId(':'.join(consumerId.split(':')[:3]))
                            consumerConnection.addConsumer(consumer)
                        
                            if spacedStrings[0] == 'SENDING:':
                                
                                direction = (logFile == connection.fromFile)
                                consumerConnection.addSentMessage(message, direction, timestamp)
                                logFile.addSentMessage(message, timestamp)
                                message.addSendingConnection(consumerConnection, direction, connection,
                                                             int(commaValues[1]), timestamp) #commaValues[1] = commandId
                                
                            elif spacedStrings[0] == 'RECEIVED:':
                                
                                direction = (logFile == connection.toFile)
                                consumerConnection.addReceivedMessage(message, direction, timestamp)
                                logFile.addReceivedMessage(message, timestamp)
                                message.addReceivingConnection(consumerConnection, direction, connection,
                                                               int(commaValues[1]), timestamp) #commaValues[1] = commandId
        
        except Exception:
            print logFile, line
            raise
            
            
    def clearData(self):
        """
        Clears all the data parsed.
        """
        
        Connection.clearData()
        Producer.clearData()
        Consumer.clearData()
        Message.clearData()
        LogFile.clearData()
                        
    def parseDirectory(self, directory):
        """
        Parses a directory of log files.
        """
        
        self.clearData()
        
        fileNames = os.walk(directory).next()[2]
        logFiles = [LogFile(directory + os.sep + fileName) for fileName in fileNames]
        
        for logFile in logFiles:
            self.parse(logFile)
            
        LogFile.closeFiles()
        
def main():
    """
    Entrance point for the command line test.
    """
    
    if len(sys.argv) != 2:
        print 'Usage: python LogParser.py directory'
    else:
        startTime = time.time()
        LogParser.getInstance().parseDirectory(sys.argv[1])
        LogParser.deleteInstance()
        print str(Message.messageCount) + ' messages parsed'
        print 'in ' + str(time.time() - startTime) + ' seconds'
        
        print 'press a key'
        sys.stdin.read(3)
        
        startTime = time.time()
        for connection in Connection.connections.itervalues():
            connection.getErrors()
            
        for logFile in LogFile.logfiles:
            logFile.getErrors()

        print 'additional: ' + str(time.time() - startTime) + ' seconds'
        time.sleep(36000)
    
if __name__ == '__main__':
    main()
    