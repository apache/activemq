"""
Module IncorrectSequencePanel
"""
import wx
from IncorrectSequenceList import IncorrectSequenceList

class IncorrectSequencePanel(wx.Panel):
    """
    This panel contains a list of incorrect events dectected by the parsing,
    and many controls to filter which events appear.
    Also the user can change if long ids (original ActiveMQConnection id strings, long)
    or short ids (a different integer for each connection) is desired.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        
        self.parent = parent
        self.options = {}
        
        self.incorrectSequenceList = IncorrectSequenceList(self)
        self.showShortIds = wx.CheckBox(self, 100, 'Show results with short ids')
        self.sentButNotReceived = wx.CheckBox(self, 101, 'Sent but not received')
        self.receivedButNotSent = wx.CheckBox(self, 102, 'Received but not sent')
        self.duplicateInConnection = wx.CheckBox(self, 103, 'Duplicate in connection')
        self.duplicateInFile = wx.CheckBox(self, 104, 'Duplicate in log file')
        self.connectionText = wx.TextCtrl(self, -1, '')
        self.rbshortId = wx.RadioButton(self, -1, style=wx.RB_GROUP, label="Short id")
        self.rblongId = wx.RadioButton(self, -1, label="Long id")
        
        self.showShortIds.SetValue(True)
        self.sentButNotReceived.SetValue(True)
        self.receivedButNotSent.SetValue(True)
        self.duplicateInConnection.SetValue(True)
        self.duplicateInFile.SetValue(True)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        
        sizer2 = wx.GridSizer()
        sizer2 = wx.GridSizer(2, 2, 5, 5)
        sizer2.AddMany([self.sentButNotReceived,
                        self.receivedButNotSent,
                        self.duplicateInConnection,
                        self.duplicateInFile
                       ])
        
        sizer3 = wx.BoxSizer(wx.HORIZONTAL)
        sizerrb = wx.BoxSizer(wx.VERTICAL)
        sizerrb.Add(self.rbshortId, 0, wx.DOWN, 5)
        sizerrb.Add(self.rblongId, 0)
        sizer3.Add(wx.StaticText(self, -1, 'Filter by connection\n(leave blank to view all)'), 0, wx.CENTER|wx.RIGHT, 5)
        sizer3.Add(self.connectionText, 1, wx.CENTER|wx.RIGHT, 5)
        sizer3.Add(sizerrb, 0, wx.CENTER|wx.RIGHT, 5)
        sizer3.Add(wx.Button(self, 105, 'Filter'), 0, wx.CENTER)
        
        sizer4 = wx.BoxSizer(wx.HORIZONTAL)
        sizer4.Add(sizer2, 0, wx.CENTER)
        sizer4.Add(sizer3, 1, wx.EXPAND|wx.CENTER|wx.LEFT, 20)
        
        sizer.Add(sizer4, 0, wx.EXPAND|wx.ALL, 5)
        
        sizer5 = wx.BoxSizer(wx.HORIZONTAL)
        sizer5.Add(self.showShortIds, 0, wx.RIGHT|wx.CENTER, 5)
        sizer5.Add(wx.Button(self, 106, 'Jump to Message Browsing'), 0, wx.CENTER)
        
        sizer.Add(sizer5, 0, wx.ALL, 5)
        
        sizer.Add(self.incorrectSequenceList, 1, wx.EXPAND)
        
        self.Bind(wx.EVT_CHECKBOX, self.OptionsChanged, id=100)
        self.Bind(wx.EVT_CHECKBOX, self.OptionsChanged, id=101)
        self.Bind(wx.EVT_CHECKBOX, self.OptionsChanged, id=102)
        self.Bind(wx.EVT_CHECKBOX, self.OptionsChanged, id=103)
        self.Bind(wx.EVT_CHECKBOX, self.OptionsChanged, id=104)
        self.Bind(wx.EVT_BUTTON, self.OnFilter, id=105)
        self.Bind(wx.EVT_BUTTON, self.OnJump, id=106)
        
        self.parseOptions()
        
        self.SetSizer(sizer)
        
    def logDataUpdated(self):
        """
        This method must be called to notify the panel that the parsed data has been updated.
        It will in turn notify the list.
        """
        
        self.incorrectSequenceList.logDataUpdated()
        
    def parseOptions(self):
        """
        Stores the values of the various checkboxes into self.options.
        """
        
        self.options['showShortIds'] = self.showShortIds.IsChecked()
        self.options['sentButNotReceived'] = self.sentButNotReceived.IsChecked() 
        self.options['receivedButNotSent'] = self.receivedButNotSent.IsChecked()
        self.options['duplicateInConnection'] = self.duplicateInConnection.IsChecked()
        self.options['duplicateInFile'] = self.duplicateInFile.IsChecked()
    
    def OptionsChanged(self, event):
        """
        Action to be executed every time one of the checkboxes is clicked.
        It calls parseOptions() and then updates the display of incorrect events.
        """
        
        self.parseOptions()
        self.incorrectSequenceList.updateList()
        
    def OnFilter(self, event):
        """
        Action to be executed every time the button 'filter' is pressed.
        """
        
        self.incorrectSequenceList.connectionForFilter = (self.connectionText.GetValue(), self.rbshortId.GetValue())
        self.OptionsChanged(event)
        
    def OnJump(self, event):
        """
        Action to be executed when the 'jump' button is pressed.
        """
        
        if self.incorrectSequenceList.GetFirstSelected() != -1:
            connectionId, messageId = self.incorrectSequenceList.GetItem(self.incorrectSequenceList.GetFirstSelected(), 2).GetText().split(' | ')
            self.parent.GetParent().browsingMessagesPanel.textctrl1.SetValue(connectionId)
            self.parent.GetParent().browsingMessagesPanel.textctrl2.SetValue(messageId)
            self.parent.GetParent().browsingMessagesPanel.rbshortId.SetValue(self.showShortIds.GetValue())
            self.parent.GetParent().browsingMessagesPanel.rblongId.SetValue(not self.showShortIds.GetValue())
            self.parent.GetParent().browsingMessagesPanel.displayMessageInfo(event)
        
            self.parent.SetSelection(1)