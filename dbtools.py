__author__ = 'Jon'
#!/usr/bin/env python

from PyCool import cool
import string

class DBTools:
    def __init__(self):
        self.db=None
        self.connected=False

    def initDB(self,file=''):
            dbSvc = cool.DatabaseSvcFactory().databaseService()
            self.db = dbSvc.openDatabase('sqlite://;schema='+file+';dbname=COOLTEST')