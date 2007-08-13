"""
Module ViewConnectionsPanel
"""
import wx
from loganalyzerengine.Connection import Connection

class ViewConnectionsPanel(wx.Panel):
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
        
        self.list = wx.ListCtrl(self, -1, 
                             style=wx.LC_REPORT|wx.LC_SINGLE_SEL|wx.LC_HRULES|wx.LC_VRULES|wx.LC_EDIT_LABELS)
        
        self.list.InsertColumn(0, 'Short id')
        self.list.InsertColumn(1, 'Long id')
        self.list.InsertColumn(2, 'Connection established FROM file')
        self.list.InsertColumn(3, 'Connection established TO file')
        self.list.InsertColumn(4, 'Producers')
        self.list.InsertColumn(5, 'Consumers')
        
        self.list.SetColumnWidth(0, 80)
        self.list.SetColumnWidth(1, 250)
        self.list.SetColumnWidth(2, 200)
        self.list.SetColumnWidth(3, 200)
        
        sizer2 = wx.BoxSizer(wx.HORIZONTAL)
        sizer2.Add(wx.Button(self, 100, 'Copy selected long id to clipboard'), 0, wx.RIGHT, 5)
        sizer2.Add(wx.Button(self, 101, "Jump to this connection's problems"))
        
        sizer.Add(sizer2, 0, wx.ALL, 5)
        sizer.Add(self.list, 1, wx.EXPAND|wx.LEFT|wx.BOTTOM|wx.RIGHT, 5)
        
        self.Bind(wx.EVT_BUTTON, self.OnCopy, id=100)
        self.Bind(wx.EVT_BUTTON, self.OnJump, id=101)
        
        self.SetSizer(sizer)
        
    def logDataUpdated(self):
        """
        Informs this panel that new data has been parsed,
        and that the list of connections should be updated.
        """
        
        self.list.DeleteAllItems()
        shortId = 0
        for longId in Connection.connectionIdList:
            connection = Connection.getConnectionByLongId(longId)
            self.insertRow(shortId, longId, connection.fromFile, connection.toFile, connection.producers, connection.consumers)
            shortId += 1
            
    def insertRow(self, shortId, longId, fromFile, to, producers, consumers):
        """
        Helper method to insert a new row in the list of connections.
        """
        
        self.list.InsertStringItem(shortId, str(shortId))
        self.list.SetStringItem(shortId, 1, str(longId))
        self.list.SetStringItem(shortId, 2, str(fromFile))
        self.list.SetStringItem(shortId, 3, str(to))
        self.list.SetStringItem(shortId, 4, ", ".join(str(p.shortId) for p in producers))
        self.list.SetStringItem(shortId, 5, ", ".join(str(c.shortId) for c in consumers))
        
    def OnCopy(self, event):
        """
        Action to be executed when pressing the 'Copy selected long id to clipboard' button.
        The longId of the selected connection will be copied to the clipboard.
        """
        
        shortId = self.list.GetFirstSelected()
        if shortId != -1 and wx.TheClipboard.Open():
            wx.TheClipboard.SetData(wx.TextDataObject(str(Connection.connectionIdList[shortId])))
            wx.TheClipboard.Close()
            
    def OnJump(self, event):
        """
        Action to be executed when pressing the 'Jump to this connection's problems' button.
        The tab with the 'IncorrectSequencePanel' will be selected and the list of incorrect
        events will be automatically filtered by the selected connection.
        """
        
        shortId = self.list.GetFirstSelected()
        if shortId != -1:
            incorrectSequencePanel = self.notebook.GetParent().incorrectSequencePanel
            incorrectSequencePanel.connectionText.SetValue(str(shortId))
            incorrectSequencePanel.rbshortId.SetValue(True)
            incorrectSequencePanel.rblongId.SetValue(False)
            incorrectSequencePanel.OnFilter(event)
            self.notebook.SetSelection(0)