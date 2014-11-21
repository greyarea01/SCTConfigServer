__author__ = 'Jon'

from PyCool import *
from coolUtils import *

# Crate uses only one folder at the moment
class Crate():
    def __init__(self,db):
        print 'Crate constructor'
        self.folder = db.getFolder('/SCT/DAQ/Config/TIM')
        # add required output fields here
        # this could also be passed from the top level I suppose
        self.outputFields = []

    def getOutput(self,iov,crate):
        # first build constraints
        # just one constraint
        if crate=='all' or crate=='list':
            constraints = []
        else:
            constraints = [('crate',int(crate))]

        results = getPayloads(self.folder,iov,iov,cool.ChannelSelection.all(),constraints)

        output = prepareOutput(iov,results,self.outputFields)
        return output
