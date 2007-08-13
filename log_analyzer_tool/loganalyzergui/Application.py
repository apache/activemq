"""
Module Application
"""
import wx
from DirectoryPanel import DirectoryPanel
from TabbedPanel import TabbedPanel

class MainPanel(wx.Panel):
    """
    Panel contained into the window of the application.
    It contains a DirectoryPanel and a TabbedPanel.
    
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        
        self.tabbedPanel = TabbedPanel(self)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(DirectoryPanel(self), 0, wx.EXPAND|wx.ALL, 5)
        sizer.Add(self.tabbedPanel, 1, wx.EXPAND)
        self.SetSizer(sizer)
        
    def logDataUpdated(self):
        """
        Method to be called when the parsed data has been updated.
        The Panel will notify its children components.
        """
        
        self.tabbedPanel.logDataUpdated()

class MainFrame(wx.Frame):
    """
    This class represents the window of the application.
    It contains a MainPanel object.
    We need to add a wx.Panel to a wx.Frame to avoid
    graphical problems in Windows.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Frame.__init__(self, parent, 100, 
                          'ActiveMQ Log Analyzer Tool', size=(1024,800))
#        ib = wx.IconBundle()
#        ib.AddIconFromFile("logparser.ico", wx.BITMAP_TYPE_ANY)
#        self.SetIcons(ib)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(MainPanel(self), 1, wx.EXPAND)
        self.SetSizer(sizer)
        self.Centre()
        self.Show(True)

class Application(wx.App):
    """
    Main class of the application
    """
    
    def OnInit(self):
        """
        To be executed when Application is launched
        """
        MainFrame(None)
        return True
