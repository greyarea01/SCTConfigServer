__author__ = 'Jon'

from PyCool import *
from coolUtils import *

# Crate uses only one folder at the moment
class Rod():
    def __init__(self,db):
        print 'Crate constructor'
        self.folder = db.getFolder('/SCT/DAQ/Config/ROD')
        # add stuff from /SCT/DAQ/Config/Slave ???
        # add required output fields here
        # this could also be passed from the top level I suppose
        self.outputFields = []

    def getOutput(self,iov,crate,slot):
        # first build constraints
        # just one constraint
        if slot=='all' or slot=='list':
            constraints = [ ('crate', int(crate))]
        else:
            constraints = [('crate',int(crate)),
                ('slot',int(slot))]

        results = getPayloads(self.folder,iov,iov,cool.ChannelSelection.all(),constraints)

        output = prepareOutput(iov,results,self.outputFields)
        return output
