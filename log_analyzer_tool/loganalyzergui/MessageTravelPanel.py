"""
Module MessageTravelPanel
"""
import wx
from MessageTravelText import MessageTravelText

class MessageTravelPanel(wx.Panel):
    """
    The function of this panel is to show the travel history of a message.
    This means the connections that the message went through.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        
        self.rbshortId = wx.RadioButton(self, -1, style=wx.RB_GROUP, label="Short id")
        self.rblongId = wx.RadioButton(self, -1, label="Long id")
        self.textctrl1 = wx.TextCtrl(self, -1, '')
        self.textctrl2 = wx.TextCtrl(self, -1, '')
        self.chkshowshortId = wx.CheckBox(self, 100, 'Show result with short ids')
        self.messageTravelPanel = MessageTravelText(self)
        
        self.chkshowshortId.SetValue(True)
        
        sizerrb = wx.BoxSizer(wx.VERTICAL)
        sizerrb.Add(self.rbshortId, 0, wx.DOWN, 5)
        sizerrb.Add(self.rblongId, 0)
        
        sizerinput = wx.BoxSizer(wx.HORIZONTAL)
        sizerinput.Add(sizerrb, 0, wx.RIGHT, 5)
        sizerinput.Add(wx.StaticText(self, -1, 'Producer id'), 0, wx.CENTER|wx.RIGHT, 5)
        sizerinput.Add(self.textctrl1, 2, wx.CENTER|wx.RIGHT, 5)
        sizerinput.Add(wx.StaticText(self, -1, 'Producer Sequence id'), 0, wx.CENTER|wx.RIGHT, 5)
        sizerinput.Add(self.textctrl2, 1, wx.CENTER|wx.RIGHT, 5)
        sizerinput.Add(wx.Button(self, 101 , 'Browse'), 0, wx.CENTER)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(sizerinput, 0, wx.EXPAND|wx.ALL, 5)
        sizer.Add(self.chkshowshortId, 0, wx.LEFT|wx.UP, 5)
        sizer.Add(self.messageTravelPanel, 1, wx.EXPAND|wx.ALL, 5)
        
        self.Bind(wx.EVT_CHECKBOX, self.displayMessageInfo, id=100)
        self.Bind(wx.EVT_BUTTON, self.displayMessageInfo, id=101)
        
        self.SetSizer(sizer)
    
    def displayMessageInfo(self, event):
        """
        Action to be executed when the 'Browse' button is pushed.
        """
        
        self.messageTravelPanel.displayMessageInfo(self.textctrl1.GetValue(),
                                                 self.textctrl2.GetValue(),
                                                 self.rbshortId.GetValue())
        
    def logDataUpdated(self):
        """
        This method must be called to notify the panel that the parsed data has been updated.
        It will in turn notify the message travel panel.
        """
        
        self.messageTravelPanel.logDataUpdated()