"""
Module MessageTravelText
"""
import wx
from loganalyzerengine.Producer import Producer
from loganalyzerengine.Message import Message

class MessageTravelText(wx.TextCtrl):
    """
    Text box where the travel path of a message is displayed.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.TextCtrl.__init__(self, parent, -1, style=wx.TE_MULTILINE)#, style=wx.TE_CENTRE)
        
        self.parent = parent
        self.datapresent = False
        
        self.SetEditable(False)
        
    def logDataUpdated(self):
        """
        Informs the text control that there is some parsed data.
        """
        
        self.datapresent = True
        
    def displayMessageInfo(self, producerId, prodSeqId, isShortId):
        """
        Displays the travel information of a message as text.
        connectionId must be a shortId if isShortId == True, and a longId if isShortId == False
        """
        
        if self.datapresent:
            
            # we run some checks on the connection id and command id,
            # and transform connectionId into a shortId
            if isShortId:
                if not producerId.isdigit():
                    wx.MessageDialog(self, 'That short producer id is not an integer', style=wx.OK).ShowModal()
                    return
                producerId = int(producerId)
                if producerId < 0 or producerId > Producer.nProducers - 1:
                    wx.MessageDialog(self, 'That short producer id does not exist', style=wx.OK).ShowModal()
                    return
            else:
                if Producer.exists(producerId):
                    producerId = Producer.longIdToShortId(producerId)
                else:
                    wx.MessageDialog(self, 'That connection id does not exist', style=wx.OK).ShowModal()
                    return
                
            if not prodSeqId.isdigit():
                wx.MessageDialog(self, 'That command id is not an integer', style=wx.OK).ShowModal()
                return
            
            # we ensure the shortId and the commandId are integers
            producerId = int(producerId)
            prodSeqId = int(prodSeqId)
            
            # we check that the message exists
            if Message.exists(Producer.getProducerByShortId(producerId), prodSeqId):
                message = Message.getMessage(Producer.getProducerByShortId(producerId), prodSeqId)
                sendingFiles, receivingFiles = message.getFiles()
                printShortIds = self.parent.chkshowshortId.GetValue()
                
                # we set the value of the text field
                self.SetValue(
                     "\n".join(['Message Id:', 
                               '\tProducer Id: ' + str(producerId if printShortIds else Producer.shortIdToLongId(producerId)), 
                               '\tProducer Sequence id: ' +  str(prodSeqId),
                               'ADVISORY' if message.advisory else '(not advisory)',
                               'Connections that sent this message:', 
                               #one line for every connection that sent a message
                               "\n".join([''.join([
                                                    '\t',
                                                    # if direction == True, message went from connection.fromFile to connection.toFile
                                                    str(connection.shortId if printShortIds else connection.longId), 
                                                    ''.join([
                                                             ', from ', 
                                                             str(connection.fromFile)
                                                             if direction else
                                                             str(connection.toFile)
                                                             , 
                                                             ' to ', 
                                                             str(connection.toFile)
                                                             if direction else
                                                             str(connection.fromFile),
                                                             ', ',
                                                             ' | '.join([''.join([
                                                                                 'ConID: ',
                                                                                 str(connection.shortId if printShortIds else connection.longId),
                                                                                 ', CommandID: ',
                                                                                 str(commandid),
                                                                                 ', ',
                                                                                 timestamp
                                                                                 ])
                                                                         for (connection, commandid, timestamp) in values
                                                                         ])
                                                             ])
                                                    ])
                                                 for (connection, direction), values in message.sendingConnections.iteritems()
                                                 ]), 
                               'Connections that received this message:', 
                               #one line for every connection that received a message
                               "\n".join([''.join([
                                                    '\t', 
                                                    # if direction == True, message went from connection.fromFile to connection.toFile
                                                    str(connection.shortId if printShortIds else connection.longId), 
                                                    ''.join([
                                                             ', from ', 
                                                             str(connection.fromFile)
                                                             if direction else
                                                             str(connection.toFile)
                                                             , 
                                                             ' to ', 
                                                             str(connection.toFile)
                                                             if direction else
                                                             str(connection.fromFile)
                                                             ,
                                                             ', ',
                                                             ' | '.join([''.join([
                                                                                 'ConID: ',
                                                                                 str(connection.shortId if printShortIds else connection.longId),
                                                                                 ', CommandID: ',
                                                                                 str(commandid),
                                                                                 ', ',
                                                                                 timestamp
                                                                                 ])
                                                                         for (connection, commandid, timestamp) in values
                                                                         ])
                                                             ])
                                                    ])
                                                 for (connection, direction), values in message.receivingConnections.iteritems()
                                                 ]), 
                               'Log files where this message was sent:', 
                               '\t' + ", ".join([str(f) for f in sendingFiles]), 
                               'Log files where this message was received:', 
                               '\t' + ", ".join([str(f) for f in receivingFiles])
                               ])
                     )
                
                
                
            else:
                # the message doesn't exist
                wx.MessageDialog(self, 'That message does not exist', style=wx.OK).ShowModal()
                return
        else:
            # there is no data present
            wx.MessageDialog(self, 'Please parse some files first', style=wx.OK).ShowModal()
            return
        
        