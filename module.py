__author__ = 'Jon'

from PyCool import *
from coolUtils import *

# this is the tricky one!
# though with the moduleAPI as well
# might be able to limit how often this gets called by the client
# since the client will normally have done an MUR query before this
# so could cache the moduleID's itself - if needed
class Module():
    def __init__(self,db):
        print 'Crate constructor'
        self.mur = db.getFolder('/SCT/DAQ/Config/MUR')
        self.rodmur = db.getFolder('/SCT/DAQ/Config/RODMUR')
        self.mod = db.getfolder('/SCT/DAQ/Config/Module')
        # add required output fields here
        # this could also be passed from the top level I suppose
        self.outputFields = []

    def getOutput(self,iov,crate,slot,position, module):
        # a JOIN would make things easy here - but not possible
        rodMurConstraints = [('crate',int(crate)),
                ('slot',int(slot)),
                ('position',int(position))]
        results = getPayloads(self.rodmur,iov,iov,cool.ChannelSelection.all(),rodMurConstraints)
        murDict = {}
        # hopefully this is only one MUR otherwise there will be trouble
        murList = []
        for (channel, since, until, dict) in results:
            obj = {'crate' : dict['crate'], 'slot' : dict['slot'], 'position' : dict['position']}
            murList[int(dict['MUR'])] = obj
            murList.append(dict['MUR'])
        if module=='all' or module=='list':
            murConstraints = [( 'MUR', murList)]
        else:
            murConstraints = [ ('MUR', murList), 'module' , int(module)]

        murResults = getPayloads(self.mur,iov,iov,cool.ChannelSelection.all(),murConstraints)
        # now we can get the moduleIDs!
        moduleIDList = []
        for (channel, since, until, dict) in murResults:
            moduleID = dict['moduleID']
            moduleIDList.append(moduleID)
        output = self.getOutputModuleID(iov,moduleIDList)
        return output

# this method is used for the moduleapi
    def getOutputModuleID(self, iov, moduleIDs):
        selection = cool.ChannelSelection # FIXME - build a channel selection from module IDs
        modConstraints = []
        modResults = getPayloads(self.mod,iov,iov, selection, modConstraints)

        output = prepareOutput(iov,modResults,self.outputFields)
        return output
