#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''Usage: 
    importData.py (-c <FILE> | --config-file <FILE>) 
    importData.py -h | --help
    
Options:
    -c <file>, --config-file <file>    Config file.
    -h, --help                         Print this text.
     
Config file template:
{
    "endPoint": "http://192.168.47.129:8080/",              # server with REST API
    "uis": "ecloud-service-uis-rest-0.3-SNAPSHOT",          # UIS name
    "mcs": "ecloud-service-mcs-rest-0.3-SNAPSHOT",          # MCS name
    "login": "admin",                                       # eCloud login
    "password": "admin",                                    # eCloud password
    "proxy": "",
    "createProvider": false,                                # create a new provider or use exists. If false than option provider["name"] must be set! 
    "provider": {"name": "",                                # name for provider or existing name (providerId)
                 "data": "{                                 # data about a new provider (must be only one line!)
                        \"organisationName\":\"\",
                        \"officialAddress\":\"\",
                        \"organisationWebsite\":\"\",
                        \"organisationWebsiteURL\":\"\",
                        \"digitalLibraryWebsite\":\"\",
                        \"digitalLibraryURL\":\"\",
                        \"contactPerson\":\"\",
                        \"remarks\":\"\"
                    }"
                },     
    "createDataSet": true,                                  # create a new data set or use exists. If true than createProvider mus be true or provider["name"] must be set!
    "dataSet": {"dataSetId": "",                            # data set name (new or exists). If set than all added files will be assigned to this data set.
                "description": ""                           # description of new dataset
               }, 
    "filesAsOneRecord": ["C:/folder/file1.txt", ...],       # list of files for add to eCloud. All files will be added to the same record.
    "files": ["C:/folder/file1.txt", ...],                  # list of files for add to eCloud. Every file will be separate record.
    "folders": ["C:/folder/Mona Lisa", ...]                 # list of folders for add to eCdloud. Folder name is used for record name and all files in this folder are added to this record.
    "complexStore" [{"recName": "Mona_Lisa",                # list of records (contain record name and file list)
                     "files": [{"repName":"edm",            # list of files (contains representation name and file path)
                                "file": "C:/..."
                                }, ...]
                     }, ...]
}

@author: Pavel Kefurt <Pavel.Kefurt@gmail.com>
@version: 2.0
'''

import sys
import pycurl
from io import BytesIO
import urllib.request
import json
import getopt
import string, random
import mimetypes
import os

 
class CloudId:
    def __init__(self, string):
        self.string = string
        tmp = json.loads(string)        
        self.id = tmp["id"]
        self.providerId = tmp['localId']["providerId"]
        self.recordId = tmp['localId']["recordId"]

class eCloud:
    
    def __init__(self, uis, mcs, address = 'http://localhost:8080/', login = "admin", password = "admin", proxy = ""):
        self.baseAddres = address
        self.uis = uis
        self.mcs = mcs
        self.auth = "%s:%s" % (login, password)
        self.login = login
        self.password = password
        self.proxy = proxy
    
    #-------------------------------
    def createProvider(self, name, data_ = '{}'):
        '''
        @param name: providerId
        @param data_: jason string consisting of either '{}' or 
            '{
                "organisationName":"",
                "officialAddress":"",
                "organisationWebsite":"",
                "organisationWebsiteURL":"",
                "digitalLibraryWebsite":"",
                "digitalLibraryURL":"",
                "contactPerson":"",
                "remarks":""
            }'
        @return: bool
        '''
        
        completUrl = "%s/data-providers/" % self.uis
        
        (response, code, _) = self.sendMessage(completUrl, urlParams={'providerId': name}, data=data_, method='POST')
        
        if code == 200 or code == 201:
            self.logInfo("Provider %s has been created." % name)
            return True
        else:
            self.logWarning("Problem with creating provider. CODE: %s RESPONSE: %s" % (code, response))
            return False
    
    #-------------------------------
    def createDataSet(self, provider, params):
        '''
        @param provider: providerId
        @param params: dictionary with keys: 
            dataSetId     - (required)
            description - (optional)
        @return: bool
        '''
        
        completUrl = "%s/data-providers/%s/data-sets/" % (self.mcs, provider)
        data_ = urllib.parse.urlencode(params)
        
        (response, code, _) = self.sendMessage(completUrl, data=data_, method='POST', contentType = 'application/x-www-form-urlencoded')
        
        if code == 201:
            self.logInfo("Dataset %s has been created." % params['dataSetId'])
            return True
        else:
            self.logWarning("Problem with creating Dataset. CODE: %s RESPONSE: %s" % (code, response))
            return False
    
    #-------------------------------
    def createCloudId(self, params):
        '''
        @param params: dictionary with keys:
            providerId    -    (required)
            recordId        -    (optional)
        @return: CloudId object or None
        '''
         
        completUrl = "%s/cloudIds/" % self.uis
             
        (response, code, _) = self.sendMessage(completUrl, urlParams=params, method='POST')

        if code == 200:
            cloudId = CloudId(response.decode("utf-8"))
            self.logInfo("CloudId %s has been created from providerId %s and recordId %s." % (cloudId.id, cloudId.providerId, cloudId.recordId))
            return cloudId
        elif code == 409: #The record already exists
            return self.getCloudId(params)
        else:
            self.logWarning("Problem with creating CloudId. CODE: %s RESPONSE: %s" % (code, response))
            return None
    
    #------------------------------- 
    def getCloudId(self, params):
        '''
        @param params: dictionary with keys:
            providerId    -    (required)
            recordId        -    (optional)
        @return: CloudId object or None
        '''
        
        completUrl = "%s/cloudIds/" % self.uis
             
        (response, code, _) = self.sendMessage(completUrl, urlParams=params, method='GET')

        if code == 200:
            cloudId = CloudId(response.decode("utf-8"))
            self.logInfo("CloudId %s has been loaded with providerId %s and recordId %s." % (cloudId.id, cloudId.providerId, cloudId.recordId))
            return cloudId
        else:
            self.logWarning("Problem with creating CloudId. CODE: %s RESPONSE: %s" % (code, response))
            return None
    
    #-------------------------------
    def createRepresentation(self, cloud, name, provider):
        '''
        @param cloud: cloudId string
        @param name: representation name
        @param provider: providerId
        @return: Representation location or ''
        '''
        
        completUrl = "%s/records/%s/representations/%s/" % (self.mcs, cloud, name)
        data_ = urllib.parse.urlencode({'providerId':provider})
             
        (response, code, headers) = self.sendMessage(completUrl, data=data_, method='POST', contentType = 'application/x-www-form-urlencoded')

        if code == 201:
            self.logInfo("Representation %s has been created for provider: %s in cloud: %s.\nRecieved URL: %s" % (name, provider, cloud, headers['Location']))
            return headers['Location']         
        else:
            self.logWarning("Problem with creating Representation. CODE: %s RESPONSE: %s" % (code, response))
            return ''
    
    #-------------------------------
    def addNewFile(self, cloud, representationName, version, filePath, fileName = None, mimeType = None):
        '''
        @param cloud: cloudId string
        @param representationName: representation name string
        @param version: version of representation
        @param filePath: path to file
        @param fileName: file name in ecloud
        @param mimeType: mime type of file
        @return: File location or '' 
        '''
        return self.addNewFileKnownUrl("%s%s/records/%s/representations/%s/versions/%s" % (self.baseAddres, self.mcs, cloud, representationName, version), filePath, fileName, mimeType)
    
    #-------------------------------
    def addNewFileKnownUrl(self, representationVersionUrl, filePath, fileName = None, mimeType = None):
        '''
        @param representationVersionUrl: URL received from self.createRepresentation
        @param filePath: path to file for send
        @param fileName: file name in ecloud
        @param mimeType: mime type of file
        @return: File location or ''
        '''
        
        completUrl = "%s/files/" % representationVersionUrl[len(self.baseAddres):]    #base addres must be erased (will be added agin in self.sendMessage)
        data_ = [('data', (pycurl.FORM_FILE, filePath))] 
        if fileName:
            data_.append(('fileName', fileName))
        if mimeType:
            data_.append(('mimeType', mimeType))
                                    
        (response, code, headers) = self.sendMessage(completUrl, data=data_, method='POST', contentType = 'multipart/form-data')
        
        if code == 201:
            self.logInfo("File %s with name %s and mimeType %s has been added.\nRecieved URL: %s" % (filePath, fileName, mimeType, headers['Location']))
            return headers['Location']        
        else:
            self.logWarning("Problem with adding file. CODE: %s RESPONSE: %s" % (code, response))
            return ''
    
    #-------------------------------
    def persistRepresentation(self, cloud, representationName, version):
        '''
        @param cloud: cloudId string
        @param representationName: representation name string
        @param version: version of representation
        @return: Persistent representation version location or ''
        '''
        return self.persistRepresentationByUrl("%s%s/records/%s/representations/%s/versions/%s" % (self.baseAddres, self.mcs, cloud, representationName, version))
    
    #-------------------------------
    def persistRepresentationByUrl(self, representationVersionUrl):
        '''
        @param representationVersionUrl: URL received from self.createRepresentation
        @return: Persistent representation version location or ''
        '''
        
        completUrl = "%s/persist/" % representationVersionUrl[len(self.baseAddres):]    #base addres must be erased (will be added agin in self.sendMessage)
                                    
        (response, code, headers) = self.sendMessage(completUrl, method='POST')

        if code == 201:
            self.logInfo("Representation version on url %s is now persistent.\nRecieved URL: %s" % (representationVersionUrl, headers['Location']))
            return headers['Location']
        if code == 405: #TODO: změnit až to upravi na jine cislo (ted konflikt už persistentního a prázdného)
            self.logInfo("Representation version on url %s is already persistent." % representationVersionUrl)    
            return ''        
        else:
            self.logWarning("Problem with adding file. CODE: %s RESPONSE: %s" % (code, response))
            return ''     
    
    #-------------------------------
    def assignRepresentationIntoDataset(self, provider, dataSet, params):
        '''
        @param provider: providerId
        @param dataset: datasetId
        @param params: dictionary with keys: 
            cloudId     - (required)
            representationName - (required)
            version - (optional) - If not provided, latest persistent version will be assigned to data set.
        @return: bool
        '''
        
        completUrl = "%s/data-providers/%s/data-sets/%s/assignments/" % (self.mcs, provider, dataSet)
        data_ = urllib.parse.urlencode(params)
             
        (response, code, _) = self.sendMessage(completUrl, data=data_, method='POST', contentType = 'application/x-www-form-urlencoded')

        if code == 204:
            self.logInfo("Representation %s in cloud % has been assigned to dataset %s" % (params['representationName'], params['cloudId'], dataSet))
            return True        
        else:
            self.logWarning("Problem with assigning representation to dataset. CODE: %s RESPONSE: %s" % (code, response))
            return False
    
    #-------------------------------    
    def sendMessage(self, url, urlParams={}, data='', method = 'GET', contentType = 'application/json'):
    
        completURL = "%s%s?%s" % (self.baseAddres, url, urllib.parse.urlencode(urlParams))

        outputBuffer = BytesIO()
        headerBuffer = BytesIO()
        
        c = pycurl.Curl()
        c.setopt(pycurl.URL, completURL)
        c.setopt(pycurl.USERPWD, self.auth)
        c.setopt(pycurl.CUSTOMREQUEST, method)
        c.setopt(c.WRITEDATA, outputBuffer)
        c.setopt(c.HEADERFUNCTION, headerBuffer.write)     
        c.setopt(c.HTTPHEADER, ['Content-Type: %s' % contentType, 'Accept-Charset: UTF-8', 'Accept: application/json']) 
        if self.proxy:
                name, port = self.proxy.split(':', 1)
                c.setopt(pycurl.PROXY, name)
                c.setopt(pycurl.PROXYPORT, int(port))
        if type(data) is str:
            c.setopt(c.POSTFIELDS, data)
        else:
            c.setopt(c.HTTPPOST, data) 
        c.perform()
        
        response = outputBuffer.getvalue()
        outputBuffer.close()
        
        headers = self.parseResponseHeaders(headerBuffer.getvalue().decode('utf-8'))
        headerBuffer.close() 
         
        return (response, c.getinfo(pycurl.HTTP_CODE), headers)
    
    #-------------------------------
    def storFiles(self, fileList, provider, dataSet = '', record = ''):
        
        #create cloudId
        params = {"providerId": provider}
        if record:
            params["recordId"] = record
        cloudId = self.createCloudId(params)

        if not cloudId:
            return
        
        representations = {}
        filesUrl = {}

        for file in fileList:
            if type(file) is dict:
                    repName = file["repName"]
                    file = file["file"]
                    
            if repName:
                representationName = repName
            else:
                representationName = file.split('.')[-1].lower()     #get suffix
            if not representationName:
                continue

            #create representation
            if not representationName in representations:
                representation = self.createRepresentation(cloudId.id, representationName, provider)
                if not representation:
                    continue
                representations[representationName] = representation

            #upload file
            fileName = os.path.basename(file)
            filesUrl[fileName] = self.addNewFileKnownUrl(representation, file, fileName, mimetypes.guess_type(file)[0])
    
        #persist representations and assign dataSet
        for (repName, rep)    in representations.items():
            self.persistRepresentationByUrl(rep) 
            if dataSet:         
                params = {"cloudId": cloudId.id, "representationName": repName, "version": self.getVersionFromRepresentationVersionUrl(rep)}
                print(params)
                self.assignRepresentationIntoDataset(provider, dataSet, params) 
        
        return (representations, filesUrl)
    
    #-------------------------------
    def parseResponseHeaders(self, headersString):
        splited = {}
        for header in headersString.split('\r\n'):
            if ':' in header:    
                    tmp = header.split(':', 1)
                    splited[tmp[0].strip()] = tmp[1].strip()
        return splited                
    
    #-------------------------------
    def getVersionFromRepresentationVersionUrl(self, repUrl):
        tmp = repUrl.split('/')
        return tmp[-1] or tmp[-2]
    
    #-------------------------------
    def getRandomName(self, size = 10, chars = string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))
    #-------------------------------    
    def logWarning(self, message):
        print("WARNING: ", message, file=sys.stderr)         

    #-------------------------------
    def logInfo(self, message):
        print("INFO: ", message, file=sys.stdout)


#-------------------------------------------------------------------------------------------------

def start(params):
    
    #--- Load params ---
    try:
        opts, args = getopt.getopt(params, "hc:", ["help", "config-file="])
    except getopt.GetoptError as err:
        print(err)
        print ('Use --help for more informations.')
        sys.exit(2)
    
    configFile = None
    for o, a in opts:
        if o in ("-h", "--help"):
            print(__doc__)
            sys.exit(2)
        elif o in ("-c", "--config-file"):
            configFile = a
    
    if not configFile:
        print("Config file not set! \nUse --help for more informations.")    
        sys.exit(2)    
    
    config = None
    try:
        with open(configFile, 'r') as f: 
            config = json.loads(f.read()) 
    except IOError as e:
        print("Problem with reading config file: %s" % e)
        print("Use --help for more informations.")
        sys.exit(2)
    except ValueError as e:
        print("Problem with config file structure: %s" % e)
        print("Use --help for more informations.")
        sys.exit(2)
    
    
    #----------------    
    providerId = ""     #necessary
    dataSetId = ""        #optional
 
    e = eCloud(config["uis"], config["mcs"], config["endPoint"], config["login"], config["password"], config["proxy"])
    
    #-- Load provider --
    if config["createProvider"]:
        if config["provider"]:
            try:
                providerId = config["provider"]["name"]
                e.createProvider(providerId, config["provider"]["data"])
            except (KeyError, TypeError) as err:
                print("Bad definition of provider.")
                print("Use --help for more informations.")
                sys.exit(2)
        else:
            providerId = e.getRandomName()
            e.createProvider(providerId)
    else:
        if config["provider"]:
            providerId = config["provider"]["name"]
        else:
            print("Provider is not defined.")
            print("Use --help for more informations.")
            sys.exit(2)
    
    #-- Load data set --
    if config["createDataSet"]:
        if not providerId:
            print("ProviderId not set.")
            print("Use --help for more informations.")
            sys.exit(2)
        if config["dataSet"]:
            try:
                dataSetId = config["dataSet"]["dataSetId"]
                e.createDataSet(providerId, config["dataSet"])
            except (KeyError, TypeError) as err:
                print("Bad definition of dataSet.")
                print("Use --help for more informations.")
                sys.exit(2)
        else:
            dataSetId = e.getRandomName()
            e.createDataSet(providerId, {"dataSetId": dataSetId})
    else:
        if config["dataSet"] and config["dataSet"]["dataSetId"]:
            dataSetId = config["dataSet"]["dataSetId"]
    
    
    #-- Store files as one record from file list --
    if config["filesAsOneRecord"]:
        e.storFiles(config["filesAsOneRecord"], providerId, dataSetId)
    
    #-- Store files from file list (every file is separate record) --
    if config["files"]:
        for file in config["files"]:
            e.storFiles([file], providerId, dataSetId)
    
    #-- Store files from folder list --
    for folder in config["folders"]:
        if os.path.isdir(folder):
            normDir = os.path.normpath(folder)
            dirName = os.path.basename(normDir)
            files = [os.path.join(normDir, f) for f in next(os.walk(normDir))[2]]
            e.storFiles(files, providerId, dataSetId, dirName)

    #-- Complex store --
    for record in config["complexStore"]:        
        e.storFiles(record["files"], providerId, dataSetId, record["recName"])
    
        
#-------------------------------
if __name__ == '__main__':
    start(sys.argv[1:])

 
    
    
    
