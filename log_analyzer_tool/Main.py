"""
Module Main
"""
from loganalyzergui.Application import Application

def main():
    """
    Entrance point for the application
    """
    app = Application(0)
    app.MainLoop()
    
if __name__ == '__main__':
    main()