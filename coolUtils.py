__author__ = 'Jon'

# return a dictionary of the payloads from a folder
# constraints should be a list of tuples of fields and values
# [ ( 'crate', [values]) , ('module', [values])] etc..
# the function will look ib the folder for each of the fields listed in constraints and test for equality
# against the value in the dictionary
#

def getPayloads(folder,iov_from,iov_to,channelSelection,constraints):
    """
    This retrieves records from a folder given the IOV range, channel selection and a set of constraints
    :rtype : list of tuples [ (channel,since,until,dictionary)]
    :param folder: folder object for the query
    :param iov_from: starting IOV
    :param iov_to: finishing IOV
    :param channelSelection: a cool.ChannelSelection
    :param constraints: a list of pairs ( 'keyname', [values] ) to apply as constraints on the query
    :return: returns a list of tuples (channel,since,until,dictionary) corresponding to each record satisfuing the query
    """
    objects = folder.browseObjects(iov_from,iov_to,channelSelection)
    # loop over the objects and process them
    outputs = []
    for obj in objects:
        # store some information about this object
        #
        channel = obj.channelId()
        since = obj.since()
        until = obj.until()
        # now get the payloads
        #

        forcePass = True
        # check to see if we have constraints to apply
        if len(constraints)!=0:
            forcePass = False

        for payload in obj.payloadIterator():
            passThis = True
            if not forcePass:
                for (name,values) in constraints:
                    passAny = False
                    for value in values:
                        passAny = passAny or checkPayload(payload,name,value)

                    passThis = passThis and passAny
            if passThis:
                dictionary = makeDictionary(payload)
                outputs.append((channel, since,until, dictionary))
        return outputs


def makeDictionary(payload):
    """
    Converts a payload in the form of an IRecord into a python dictionary
    :rtype : dictionary
    :param payload: the IRecord to convert
    :return: the dictionary coresponding to the input payload
    """
    n = payload.size()
    skip = False
    dictionary={}
    for i in range(0,n-1):
        field = payload.field(i)
        name = field.name()
        fieldType = field.storageType().name()
        if field.isNull():
            val = 0
            skip=True
        else:
            val = field.data(type)()
        if fieldType=='UChar':
            val = ord(val)
        dictionary[name]=val
    if skip:
        return {}
    else:
        return dictionary


def checkPayload(payload,name,value):
    """
    Compares the value of the specified field with the given value in the payload passed as argument
    :rtype : object
    :param payload: a payload object - this is an IRecord
    :param name: field that should be compared
    :param value: value to check for equality
    :return: True if the field has the required value, False otherwise
    """
    field = payload.field(name)
    fieldType = field.storageType().name()
    if field.isNull():
        return False

    val = field.data(type)()
    if fieldType == 'UChar':
        val = ord(val)
    return val == value


# prepare output for multiple payloads payload
def prepareOutput(iov, dictionaries, outputFields):
    """
    Converts the list of dictionaries from getPayloads and getPayloadsRange into a suitable format for
    turning into JSON and sending to the client
    :rtype : dictionary suitable for parsing into JSON for the client
    :param iov: interval of validity in the original query
    :param dictionaries: list of dictionaries that are the result of the query
    :param outputFields: list of fields to include in the output from the dictionaries
    """
    output= {'iov': iov, 'headers': outputFields[:], 'rows': []}
    # every 'row' has the same det of fields so can store them here
    # this looks more like a table then
    for (channel, since, until, dictionary) in dictionaries:
        obj = {'COOLchan': channel, 'since': since, 'until': until}
        # dictionaries  = [ 'channel' : [ (since, until, payloadDictionary) ] ]
    # each of these is like a row in the DB query response
        for key in outputFields:
            val = dictionary[key]
            obj['values'].append(val)
        output['rows'].append(obj)

