"""
Module TabbedPanel
"""
import wx
from IncorrectSequencePanel import IncorrectSequencePanel
from MessageTravelPanel import MessageTravelPanel
from ViewClientsPanel import ViewClientsPanel
from ViewConnectionsPanel import ViewConnectionsPanel
from ViewFilesPanel import ViewFilesPanel

class TabbedPanel(wx.Panel):
    """
    Panel with the tabs that will display the information once the log files are parsed.
    It contains 4 tabs, via a wx.Notebook object:
        -IncorrectSequencePanel.
        -BrowsingMessagesPanel.
        -ViewConnectionsPanel.
        -ViewFilesPanel.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        
        notebook = wx.Notebook(self, -1)
        
        self.incorrectSequencePanel = IncorrectSequencePanel(notebook)
        self.browsingMessagesPanel = MessageTravelPanel(notebook)
        self.viewClientsPanel = ViewClientsPanel(notebook)
        self.viewConnectionsPanel = ViewConnectionsPanel(notebook)
        self.viewFilesPanel = ViewFilesPanel(notebook)
        
        notebook.AddPage(self.incorrectSequencePanel, 'Incorrect Sequences')
        notebook.AddPage(self.browsingMessagesPanel, 'Message Browsing')
        notebook.AddPage(self.viewClientsPanel, 'View clients')
        notebook.AddPage(self.viewConnectionsPanel, 'View connections')
        notebook.AddPage(self.viewFilesPanel, 'View files')
        
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(notebook, 1, wx.EXPAND|wx.ALL, 5)
        self.SetSizer(sizer)
        
    def logDataUpdated(self):
        """
        When this panel is notified that the parsed data has changed,
        it notifies the 4 sub panels.
        """
        
        self.incorrectSequencePanel.logDataUpdated()
        self.browsingMessagesPanel.logDataUpdated()
        self.viewClientsPanel.logDataUpdated()
        self.viewConnectionsPanel.logDataUpdated()
        self.viewFilesPanel.logDataUpdated()