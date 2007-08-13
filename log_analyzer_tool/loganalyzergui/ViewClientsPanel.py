"""
Module ViewConnectionsPanel
"""
import wx

from loganalyzerengine.Connection import Connection
from loganalyzerengine.Producer import Producer
from loganalyzerengine.Consumer import Consumer

class ViewClientsPanel(wx.Panel):
    """
    This panel shows the list of connections that appear in the log files.
    Also, it enables the user to copy the long id of a connection to the system clipboard,
    and to 'jump' to the IncorrectSequencePanel, filtering the display so that only the
    events of that connection are displayed.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        
        self.notebook = parent
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        
        self.producerList = wx.ListCtrl(self, -1, 
                             style=wx.LC_REPORT|wx.LC_SINGLE_SEL|wx.LC_HRULES|wx.LC_VRULES|wx.LC_EDIT_LABELS)
        
        self.producerList.InsertColumn(0, 'Short id')
        self.producerList.InsertColumn(1, 'Short Connection id')
        self.producerList.InsertColumn(2, 'Session Id')
        self.producerList.InsertColumn(3, 'Value')
        self.producerList.InsertColumn(4, 'Long Connection id')
        
        self.producerList.SetColumnWidth(0, 80)
        self.producerList.SetColumnWidth(1, 120)
        self.producerList.SetColumnWidth(2, 80)
        self.producerList.SetColumnWidth(3, 80)
        self.producerList.SetColumnWidth(4, 500)
        
        self.consumerList = wx.ListCtrl(self, -1, 
                             style=wx.LC_REPORT|wx.LC_SINGLE_SEL|wx.LC_HRULES|wx.LC_VRULES|wx.LC_EDIT_LABELS)
        
        self.consumerList.InsertColumn(0, 'Short id')
        self.consumerList.InsertColumn(1, 'Short Connection id')
        self.consumerList.InsertColumn(2, 'Session Id')
        self.consumerList.InsertColumn(3, 'Value')
        self.consumerList.InsertColumn(4, 'Long Connection id')
        
        self.consumerList.SetColumnWidth(0, 80)
        self.consumerList.SetColumnWidth(1, 120)
        self.consumerList.SetColumnWidth(2, 80)
        self.consumerList.SetColumnWidth(3, 80)
        self.consumerList.SetColumnWidth(4, 500)
        
        sizer.Add(wx.StaticText(self, -1, 'Producers'), 0, wx.CENTER|wx.LEFT|wx.TOP|wx.RIGHT, 15)
        sizer.Add(self.producerList, 1, wx.EXPAND|wx.ALL, 5)
        sizer.Add(wx.StaticText(self, -1, 'Consumers'), 0, wx.CENTER|wx.LEFT|wx.TOP|wx.RIGHT, 15)
        sizer.Add(self.consumerList, 1, wx.EXPAND|wx.ALL, 5)
        
        self.SetSizer(sizer)
        
    def logDataUpdated(self):
        """
        Informs this panel that new data has been parsed,
        and that the list of connections should be updated.
        """
        
        self.producerList.DeleteAllItems()
        self.consumerList.DeleteAllItems()
        
        shortId = 0
        for longId in Producer.producerIdList:
            producer = Producer.getProducerByLongId(longId)
            self.insertRow(self.producerList, shortId, Connection.longIdToShortId(producer.connectionId),
                           producer.sessionId, producer.value,
                           Connection.getConnectionByLongId(producer.connectionId))
            shortId += 1
            
        shortId = 0
        for longId in Consumer.consumerIdList:
            consumer = Consumer.getConsumerByLongId(longId)
            self.insertRow(self.consumerList, shortId, Connection.longIdToShortId(consumer.connectionId),
                           consumer.sessionId, consumer.value,
                           Connection.getConnectionByLongId(consumer.connectionId))
            shortId += 1
            
    def insertRow(self, targetList, shortId, shortConnectionId, sessionId, value, longConnectionId):
        """
        Helper method to insert a new row in the list of connections.
        """
        
        targetList.InsertStringItem(shortId, str(shortId))
        targetList.SetStringItem(shortId, 1, str(shortConnectionId))
        targetList.SetStringItem(shortId, 2, str(sessionId))
        targetList.SetStringItem(shortId, 3, str(value))
        targetList.SetStringItem(shortId, 4, str(longConnectionId))