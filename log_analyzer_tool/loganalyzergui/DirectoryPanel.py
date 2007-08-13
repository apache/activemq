"""
Module DirectoryPanel
"""
import wx
import os

from loganalyzerengine.LogParser import LogParser

class DirectoryPanel(wx.Panel):
    """
    Panel to choose the directory with the log files to be parsed,
    and launch the parsing / analyzing process.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        self.mainPanel = parent
        self.textctrl = wx.TextCtrl(self, -1, 'C:\logs')
        
        self.lastDirectoryOpen = ''
        
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(wx.StaticText(self, -1, 'Directory'), 0, wx.CENTER|wx.RIGHT, 5)
        sizer.Add(self.textctrl, 1, wx.CENTER|wx.RIGHT, 5)
        sizer.Add(wx.Button(self, 100 , 'Choose'), 0, wx.CENTER|wx.RIGHT, 5)
        sizer.Add(wx.Button(self, 101 , 'Parse'), 0, wx.CENTER)
        
        self.Bind(wx.EVT_BUTTON, self.OnChoose, id=100)
        self.Bind(wx.EVT_BUTTON, self.OnParse, id=101)
        
        self.SetSizer(sizer)
        
    def OnChoose(self, event):
        """
        Action to be executed when the 'Choose' button is pressed.
        """
        
        dialog = wx.DirDialog(self, defaultPath=self.lastDirectoryOpen)
        if dialog.ShowModal() == wx.ID_OK:
            self.textctrl.SetValue(dialog.GetPath())
            self.lastDirectoryOpen = dialog.GetPath()
        else:
            wx.MessageDialog(self, 'Please choose an appropiate directory', style=wx.OK).ShowModal()
        
    def OnParse(self, event):
        """
        Action to be executed when the 'Parse' button is pressed.
        """
        
        path = self.textctrl.GetValue()
        if os.path.isdir(path):
            LogParser.getInstance().parseDirectory(path)
            self.mainPanel.logDataUpdated()
            LogParser.deleteInstance()
        else:
            wx.MessageDialog(self, 'That directory does not exist', style=wx.OK).ShowModal()
    