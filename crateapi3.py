__author__ = 'Jon'

#!/usr/bin/env python

from crate import Crate
from rod import Rod

#from rod import RodIOV
#from mur import MurIOV
#from module import ModuleIOV
#from chip import ChipIOV


def attributeListToDict(attributeList):
    dictionary = {}
    for attribute in iter(attributeList):
        name=attribute.specification().name()
        value=attribute.data()
        dictionary[name]=value

    return dictionary


class CrateApi:
    def __init__(self,db):
        #self.session=db.session
        self.session=None
        self.db=db
        self.crate = Crate(db)
        self.rod = Rod(db)

    def getInfo(self,iov='now',crate='list',slot='none',position='none',module='none',chip='none'):
        print 'getInfo called: '+iov+' '+crate+' '+slot+' '+position+' '+module+' '+chip
        if crate in ['none','list','all']:
            return self.getCrateInfo(iov,crate)
        if slot=='none':
            return self.getCrateInfo(iov,crate)
        elif slot in ['list','all']:
            return self.getRODInfo(iov,crate,slot)

        if position=='none':
            return self.getRODInfo(iov,crate,slot)
        elif position in ['list','all']:
            return self.getMURInfo(iov,crate,slot,position)

        if module=='none':
            return self.getMURInfo(iov,crate,slot,position)

        if chip=='none':
            return self.getMODInfo(iov,crate,slot,position,module)

        return self.getChipInfo(iov,crate,slot,position,module,chip)

    def getCrateInfo(self,iov,crate='list'):
        return self.crate.getOutput(iov,crate)

    def getRODInfo(self,iov,crate='none',slot='list'):
        return self.rod.getOutput(iov,crate,slot)

    def getMURInfo(self,iov,crate='none',slot='none',position='list'):
        reducedForm=False
        if crate in ['none','list','all']:
            return None
        if slot in ['none','list','all']:
            return None
        if position=='none':
            return None
        elif position=='list':
            position=-1
            reducedForm=True
        elif position=='all':
            reducedForm=False
            position=-1
        constraints={ 'crate':int(crate), 'slot':int(slot), 'position':int(position)}
        return self.getRows(MurIOV(self.db,iov),constraints,reducedForm)

   def getMODInfo(self,iov,crate='none',slot='none',position='none',module='list'):
        print 'getMODInfo : '+module
        reducedForm=False
        if crate in ['none','list','all']:
            return None
        if slot in ['none','list','all']:
            return None
        if position in ['none','list','all']:
            return None

        if module == 'none':
            return None
        elif module=='list':
            module=-1
            reducedForm=True
        elif module=='all':
            reducedForm=False
            module=-1

        constraints={ 'crate':int(crate), 'slot':int(slot), 'position':int(position), 'module':int(module)}
        modIOV=ModuleIOV(self.db,iov)

        rows=self.getRows(modIOV,constraints,reducedForm)
        # now need to do second query to get extra information
        # weird structure of COOL + foreign tables means can't do a single JOIN to get this info
        # bizarre structure
        headers=rows['headers']
        index1=headers.index('moduleID')
        index2=headers.index('rmoduleID')
        modules=set()

        print 'Found '+str(len(rows['rows']))+' entries'
        for row in rows['rows']:
            values=row['values']
            modules.add(values[index1])
            modules.add(values[index2])
        for module in modules:
            print module

        modIOV.updateIOV(iov,modules) #this triggers the extended form

        rows = self.getRows(modIOV,constraints,reducedForm)

        return rows
        #return self.getRows(ModuleIOV(self.db,iov),constraints,reducedForm)

  def getChipInfo(self,iov,crate='none',slot='none',position='none',moduleID='none',chip='list'):
        reducedForm=False
        if crate in ['none','list','all']:
            return None
        if slot in ['none','list','all']:
            return None
        if position in ['none','list','all']:
            return None
        if moduleID in ['none','list','all']:
            return None
        if chip == 'none':
            return None
        elif chip=='list':
            chip=-1
            reducedForm=True
        elif chip=='all':
            reducedForm=False
            chip=-1
        constraints={'chip':chip}
        chipIOV=ChipIOV(self.db,iov)
        chipIOV.updateIOV(iov,moduleID)
        rows = self.getRows(chipIOV,constraints,reducedForm)
        return rows


  def getRows(self,module,constraints,reducedForm):
        session = self.db.getSession()
        #if self.session is None:
        #    self.session=self.db.getSession()

        #schema=self.session.nominalSchema()
        #self.session.transaction().start(True)
        schema=session.nominalSchema()
        session.transaction().start(True)
        query=schema.newQuery()

        module.buildConditions(query,constraints)
        module.buildOutputList(query,reducedForm)

        cursor=query.execute()
        rows=[]
        for row in iter(cursor):
            dict=attributeListToDict(row)
            rows.append(dict)
        del query
        #self.session.transaction().commit()
        session.transaction().commit()
        output = module.prepareOutput(rows)
        return output

