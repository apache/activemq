"""
Module IncorrectSequenceList
"""
import wx
from loganalyzerengine.Connection import Connection
from loganalyzerengine.LogFile import LogFile

def advisoryString(message):
    """
    Helper method
    """
 
    if message.advisory:
        return ' (ADVISORY)'
    else:
        return ''
        

class IncorrectSequenceList(wx.ListCtrl):
    """
    List of the incorrect events detected after parsing the logs.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.ListCtrl.__init__(self, parent, -1, 
                             style=wx.LC_REPORT|wx.LC_SINGLE_SEL|wx.LC_HRULES|wx.LC_VRULES)
        
        self.incorrectSequencePanel = parent
        
        self.datapresent = False
        self.connectionForFilter = None
        
        self.InsertColumn(0, 'Type')
        self.InsertColumn(1, 'Connection id / File')
        self.InsertColumn(2, 'Message id (prod id | prod seq id)')
        self.InsertColumn(3, 'ntimes')
        self.InsertColumn(4, 'timestamps')
        
        self.SetColumnWidth(0, 150)
        self.SetColumnWidth(1, 250)
        self.SetColumnWidth(2, 300)
        self.SetColumnWidth(3, 100)
        self.SetColumnWidth(4, 300)
        
        
    def logDataUpdated(self):
        """
        This method must be called to notify the list that the parsed data
        has changed.
        """

        self.datapresent = True
        self.updateList()
        
    def checkConnectionForFilter(self):
        """
        Returns True if the connection that was inputed by the user
        to filter the events is valid.
        self.connectionForFilter is a (string, boolean) tuple.
        The boolean value is True if the string is a shortId, and False if it is a long id.
        """
        
        if self.connectionForFilter is None:
            return False
        
        if self.connectionForFilter[1]:
            #shortId
            return self.connectionForFilter[0].isdigit() and \
                   int(self.connectionForFilter[0]) > -1 and \
                   int(self.connectionForFilter[0]) < len(Connection.connectionIdList)
        
        else:
            #longId
            return self.connectionForFilter[0] in Connection.connections
        
    def updateList(self):
        """
        Updates the display of the list of incorrect events
        """
        
        self.DeleteAllItems()
        if self.datapresent:
            options = self.incorrectSequencePanel.options
            
            row = 0
            
            # we construct a list of connection long ids to be displayed,
            # depending on the filter desired
            if self.checkConnectionForFilter():
                if self.connectionForFilter[1]:
                    #shortId
                    connectionIds = [Connection.connectionIdList[int(self.connectionForFilter[0])]]
                else:
                    connectionIds = [self.connectionForFilter[0]]
            else:
                if self.connectionForFilter is None or self.connectionForFilter[0] == '':
                    connectionIds = Connection.connections.keys()
                else:
                    connectionIds = []      
                
            # we display the problems tied to connections
            showShortIDs = options['showShortIds']
            
            for longId in connectionIds:
                
                # we display long or short ids depending on the option chosen
                connection = Connection.getConnectionByLongId(longId)
                errors = connection.getErrors()
                
                if showShortIDs:
                    printedConnectionId = connection.shortId
                else:
                    printedConnectionId = longId
                
                # sent but not received messages
                if options['sentButNotReceived']:
                    for storedMessage, n, timestamps in errors[0]:
                        message = storedMessage[0]
                        self.insertRow(row, 'sentButNotReceived' + advisoryString(message), printedConnectionId,
                                       message.producer.shortId if showShortIDs else message.producer.longId,
                                       message.prodSeqId, n, timestamps, wx.WHITE)
                        row += 1
                
                # received but not sent messages
                if options['receivedButNotSent']:
                    for storedMessage, n, timestamps in errors[1]:
                        message = storedMessage[0]
                        self.insertRow(row, 'receivedButNotSent' + advisoryString(message), printedConnectionId,
                                       message.producer.shortId if showShortIDs else message.producer.longId,
                                       message.prodSeqId, n, timestamps, wx.WHITE)
                        row += 1
                
                # duplicate sent or received messages through a connection
                if options['duplicateInConnection']:
                    for storedMessage, n, timestamps in errors[2]:
                        message = storedMessage[0]
                        self.insertRow(row, 'duplicateSentInConnection' + advisoryString(message), printedConnectionId,
                                       message.producer.shortId if showShortIDs else message.producer.longId,
                                       message.prodSeqId, n, timestamps, wx.WHITE)
                        row += 1
                        
                    for storedMessage, n, timestamps in errors[3]:
                        message = storedMessage[0]
                        self.insertRow(row, 'duplicateReceivedInConnection' + advisoryString(message), printedConnectionId,
                                       message.producer.shortId if showShortIDs else message.producer.longId,
                                       message.prodSeqId, n, timestamps, wx.WHITE)
                        row += 1
            
            # duplicate sent or received messages in the same log file.
            # right now they are only shown when the connection filter is not used.
            if options['duplicateInFile'] and not self.checkConnectionForFilter() and \
            (self.connectionForFilter is None or self.connectionForFilter[0] == ''):
                for logfile in LogFile.logfiles:
                    errors = logfile.getErrors()
                    
                    for message, n, timestamps in errors[0]:
                        self.insertRow(row, 'duplicateSentInFile' + advisoryString(message), str(logfile),
                                       message.producer.shortId if showShortIDs else message.producer.longId,
                                       message.prodSeqId, n, timestamps, wx.WHITE)
                        row += 1
                        
                    for message, n, timestamps in errors[1]:
                        self.insertRow(row, 'duplicateReceivedInFile' + advisoryString(message), str(logfile),
                                       message.producer.shortId if showShortIDs else message.producer.longId,
                                       message.prodSeqId, n, timestamps, wx.WHITE)
                        row += 1
            
    def insertRow(self, rownumber, typeOfError, connectionId, producerId, producerSequenceId, n, timestamps, col):
        """
        Helper method to insert a row into the list
        """
        
        self.InsertStringItem(rownumber, typeOfError)
        self.SetStringItem(rownumber, 1, str(connectionId))
        self.SetStringItem(rownumber, 2, str(producerId) + ' | ' + str(producerSequenceId))
        self.SetStringItem(rownumber, 3, str(n))
        self.SetStringItem(rownumber, 4, ' | '.join(timestamps))
        self.SetItemBackgroundColour(rownumber, col)

        