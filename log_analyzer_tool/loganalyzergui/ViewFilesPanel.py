"""
Module ViewFilesPanel
"""
import wx
from loganalyzerengine.LogFile import LogFile

class ViewFilesPanel(wx.Panel):
    """
    This panel shows the list of log files that have been read.
    """
    
    def __init__(self, parent):
        """
        Constructor
        """
        
        wx.Panel.__init__(self, parent, -1)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        
        self.text = wx.TextCtrl(self, -1, style=wx.TE_MULTILINE)
        
        sizer.Add(self.text, 1, wx.EXPAND|wx.ALL, 5)
        
        self.SetSizer(sizer)
        
    def logDataUpdated(self):
        """
        The panel is informed that new data has been parsed,
        and the list of files should be updated.
        """
        
        self.text.SetValue(
            '\n'.join(
                      '\n'.join([
                                 str(file),
                                 '\tConnections established from this file:',
                                 '\n'.join(['\t\t' + str(con.shortId) + ' ' + str(con.longId) for con in file.outgoing]),
                                 '\tConnections established to this file:',
                                 '\n'.join(['\t\t' + str(con.shortId) + ' ' + str(con.longId) for con in file.incoming])
                                ])
                      for file in LogFile.logfiles)
        )
            
        