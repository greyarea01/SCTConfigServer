__author__ = 'Jon'

from PyCool import *
from coolUtils import *

class Mur():
    def __init__(self,db):
        print 'Crate constructor'
        self.mur = db.getFolder('/SCT/DAQ/Config/MUR')
        self.rodmur = db.getFolder('/SCT/DAQ/Config/RODMUR')

        # add stuff from /SCT/DAQ/Config/Slave ???
        # add required output fields here
        # this could also be passed from the top level I suppose
        self.outputFields = []

    def getOutput(self,iov,crate,slot,position):
        # first build constraints
        # just one constraint
        if position=='all' or position=='list':
            constraints = [('crate',int(crate)),
                ('slot',int(slot))]
        else:
            constraints = [('crate',int(crate)),
                ('slot',int(slot)),
                ('position',int(position))]

        # ideally we'd do a JOIN here since this is how the tables are structured
        # but COOL doesn't do JOINs so we have to get every record and do the JOIN by hand
        #
        # first get the MUR values
        results = getPayloads(self.rodmur,iov,iov,cool.ChannelSelection.all(),constraints)
        # this method almost certainly doesn't in principle work for a range of IOVs
        # since there can be arbitrary overlaps of the IOV for the RODMUR and MUR folders
        # would have to construct the set of 100% overlapping ranges for the two folders
        murDict = {}
        murConstraints = {'MUR':[]}
        for (channel, since, until, dict) in results:
            obj = {'crate' : dict['crate'], 'slot' : dict['slot'], 'position' : dict['position']}
            murList[int(dict['MUR'])] = obj
            murConstraints['MUR'].append(dict['MUR'])

        murResults = getPayloads(self.mur,iov,iov,cool.ChannelSelection.all(),murConstraints)
        # now merge in the crate, slot and position info
        for (channel, since, until, dict) in murResults:
            murValue = int(dict['MUR'])
            obj = murDict[murValue]
            dict = dict + obj

        output = prepareOutput(iov,murResults,self.outputFields)
        return output
