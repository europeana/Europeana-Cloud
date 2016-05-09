#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''Usage: 
    prepareAll.py (-c <FILE> | --config-file <FILE>) 
    prepareAll.py -h | --help
    
Options:
    -c <file>, --config-file <file>    Config file.
    -h, --help                         Print this text.
     
Config file template:
{
    "importConfigFile": "/path/to/importData/config/config.txt",
    "solrHome": "/etc/solr",
    "coreName": "testCore",
    "elasticsearchUrl": "http://localhost:9200",
    "indexName": "test_index",
    "typeName": "testType",
    "kafkaHome": "/etc/kafka",
    "zookeeper": "localhost:2181",
    "topics": ["text_stripping", "extraction_topic", "index_topic", "storm_metrics_topic"],
    "stormHome": "/etc/storm",
    "topologies": [{"jar": "/home/ceffa/topologies/text_topology.jar", 
                    "mainClass": "eu.europeana.cloud.service.dps.storm.topologies.text.TextStrippingTopology",
                    "name": "text_topology",
                    "jvmParams": "\"-Dhttp.proxyHost=wwwcache.open.ac.uk -Dhttp.proxyPort=80\""
                    }, {
                    "jar": "/home/ceffa/topologies/index_topology.jar",
                    "mainClass": "eu.europeana.cloud.service.dps.storm.topologies.indexer.IndexerTopology",
                    "name": "index_topology",
                    "jvmParams": ""
                    }],
    "dps": "kmi-web29.open.ac.uk:8082/ecloud-service-dps-rest-0.4-SNAPSHOT",
    "task": {"topic": "text_stripping",
             "taskName": "NewDataset",
             "providerId": "ceffa",
             "datasetId": "ceffa_dataset1",
             "indexerName": "SOLR_INDEXER",
             "index": "solrCore",
             "type": "",
             "extractText": "true",
             "indexData": "true",
             "storeExtracted": "false",
             "login": "login",
             "password": "password"},
    "proxy": "wwwcache.open.ac.uk:80"
}

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Following services have to be started before run this script:
    Solr
    Elasticsearch
    Kafka
    Zookeeper
    Storm
    eCloud REST API (UIS, MCS, DPS)
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

!!! Recommendation: run this script with sudo permitions !!!

@author: Pavel Kefurt <Pavel.Kefurt@gmail.com>
@version: 1.0
'''

import sys
import json
import getopt
from subprocess import call
import codecs
import pycurl
from io import BytesIO
import urllib.request
import importData

solrSchema = '''<schema name="minimal" version="1.5">

    <field name="id" type="string" indexed="true" stored="true" required="true" />
    <field name="raw_text" type="text_general" indexed="true" stored="true" termVectors="true" required="false" />
    
    <dynamicField name="*" type="text_general" indexed="true" stored="true" termVectors="true" multiValued="true" />
    <uniqueKey>id</uniqueKey>

    <fieldType name="string" class="solr.StrField"/>
    
    <fieldType name="text_general" class="solr.TextField" positionIncrementGap="100">
      <analyzer type="index">
        <tokenizer class="solr.StandardTokenizerFactory"/>
      </analyzer>
      <analyzer type="query">
        <tokenizer class="solr.StandardTokenizerFactory"/>
      </analyzer>
    </fieldType>
    
</schema>
'''

solrConfig = '''<config>

  <dataDir>${solr.data.dir:}</dataDir>

  <directoryFactory name="DirectoryFactory"
                    class="${solr.directoryFactory:solr.NRTCachingDirectoryFactory}"/>

  <luceneMatchVersion>${tests.luceneMatchVersion:LATEST}</luceneMatchVersion>

  <updateHandler class="solr.DirectUpdateHandler2">
    <commitWithin>
      <softCommit>${solr.commitwithin.softcommit:true}</softCommit>
    </commitWithin>

  </updateHandler>
  <requestHandler name="/select" class="solr.SearchHandler">
    <lst name="defaults">
      <str name="echoParams">explicit</str>
      <str name="indent">true</str>
      <str name="df">text</str>
    </lst>

  </requestHandler>

  <requestHandler name="/admin/" class="org.apache.solr.handler.admin.AdminHandlers" />

  <requestHandler name="/update" class="solr.UpdateRequestHandler"  />
</config>
'''

elasticsearchMapping = '''
{"mappings": {"%s":
 {
  "properties": {
        "raw_text":{
           "type": "string",
          "term_vector": "with_positions_offsets"
        }
    }
 }
}
}
'''

dpsTask_dataset = '''{{
"inputData":{{}},
"parameters":
    {{"INDEXER":"{{
        \\"INDEXER_NAME\\":\\"{2}\\",
        \\"INDEX\\":\\"{3}\\",
        \\"TYPE\\":\\"{4}\\"}}",
    "EXTRACT_TEXT":"{5}",
    "INDEX_DATA":"{6}",
    "STORE_EXTRACTED_TEXT":"{7}",
    "PROVIDER_ID":"{0}",
    "DATASET_ID":"{1}"}},
"taskName":"NewDataset"}}
'''

dpsTask_file = '''{{
"inputData":{{}},
"parameters":
    {{"INDEXER":"{{
        \\"INDEXER_NAME\\":\\"{1}\\",
        \\"INDEX\\":\\"{2}\\",
        \\"TYPE\\":\\"{3}\\"}}",
    "EXTRACT_TEXT":"{4}",
    "INDEX_DATA":"{5}",
    "STORE_EXTRACTED_TEXT":"{6}",
    "FILE_URL":"{0}"}},
"taskName":"NewFile"}}
'''

#-------------------------------
def prepareSolr(solrHome, coreName):
    ''
    createCommand = "{0}/bin/solr create -c {1}".format(solrHome, coreName)
    restartCommand = "{0}/bin/solr restart".format(solrHome)
    coreConfigFolder = "{0}/server/solr/{1}/conf/".format(solrHome, coreName)
    
    #create core
    if call(createCommand, shell=True):
        print("Cannot create a new Solr core.")

    #create schema
    with codecs.open(coreConfigFolder+"schema.xml", "w", "utf-8") as schemaFile:
        schemaFile.write(solrSchema)
    
    #create solrConfig
    with codecs.open(coreConfigFolder+"solrconfig.xml", "w", "utf-8") as solrconfigFile:
        solrconfigFile.write(solrConfig)
    
    #restart solr
    if call(restartCommand, shell=True):
        print("Cannot restart Solr.")


#-------------------------------
def prepareElasticsearch(esUrl, indexName, typeName, proxyString=""):
    ''
    #send mapping to elasticsearch
    data = elasticsearchMapping % typeName
    response, code, _ = sendMessage(esUrl+"/%s"%indexName, data=data, method='PUT', proxy=proxyString)
    if code != 200:
        print("Problem with elasticsearch mapping.")
        print(response.decode('utf-8'))


#-------------------------------  
def sendMessage(url, urlParams={}, data='', method = 'GET', contentType = 'application/json', 
                auth = None, proxy = None):

    if urlParams:
        completURL = "%s?%s" % (url, urllib.parse.urlencode(urlParams))
    else:
        completURL = url
    
    outputBuffer = BytesIO()
    headerBuffer = BytesIO()
    
    c = pycurl.Curl()
    c.setopt(pycurl.URL, completURL)
    c.setopt(pycurl.CUSTOMREQUEST, method)
    c.setopt(c.WRITEDATA, outputBuffer)
    c.setopt(c.HEADERFUNCTION, headerBuffer.write)   
    c.setopt(c.HTTPHEADER, ['Content-Type: %s' % contentType, 'Accept-Charset: UTF-8', 'Accept: application/json']) 
    c.setopt(c.NOPROXY, "localhost")
    if auth:
        c.setopt(pycurl.USERPWD, auth)
    if proxy:
        name, port = proxy.split(':', 1)
        c.setopt(pycurl.PROXY, name)
        c.setopt(pycurl.PROXYPORT, int(port))
    if type(data) is str:
        c.setopt(c.POSTFIELDS, data)
    else:
        c.setopt(c.HTTPPOST, data) 
    c.perform()
    
    response = outputBuffer.getvalue()
    outputBuffer.close()
    
    headers = parseResponseHeaders(headerBuffer.getvalue().decode('utf-8'))
    headerBuffer.close() 
   
    return (response, c.getinfo(pycurl.HTTP_CODE), headers)


#-------------------------------
def parseResponseHeaders(headersString):
    splited = {}
    for header in headersString.split('\r\n'):
        if ':' in header:  
            tmp = header.split(':', 1)
            splited[tmp[0].strip()] = tmp[1].strip()
    return splited  


#-------------------------------
def prepareKafkaTopics(kafkaHome, zookeeper, topics):
    ''
    createCommandTemplate = "{0}/bin/kafka-topics.sh --create --zookeeper {1} --replication-factor 1 --partitions 1 --topic {2}"
        
    for topicName in topics:     
        createCommand = createCommandTemplate.format(kafkaHome, zookeeper, topicName) 
        if call(createCommand, shell=True):
            print("Cannot create topic {0}.".format(topicName))
    

#-------------------------------
def submitTopology(stormHome, topologyJar, mainClass, topologyName, jvmParams=""):
    ''
    submitCommandTemplate = "{0}/bin/storm jar {1} {2} {3} 1 1 {4}"
    
    submitCommand = submitCommandTemplate.format(stormHome, topologyJar, mainClass, topologyName, jvmParams) 
    if call(submitCommand, shell=True):
        print("Cannot submit topology {0}.".format(topologyName))
    

#-------------------------------
def setPermitionForTopology(dps, topology, login, password, proxyString=""):
    ''
    message = "username={0}".format(login)
    url = "{0}/topologies/{1}/permit/".format(dps, topology)
    authString = "{0}:{1}".format(login, password)

    response, code, _ = sendMessage(url, data=message, method='POST', contentType='application/x-www-form-urlencoded',
                                     auth=authString, proxy=proxyString)
    if code != 200:
        print("Problem with topology permition.")
        print(response.decode('utf-8'))
        

#-------------------------------
def sendDpsTask(dpsBase, topic, message, login, password, proxyString=""):
    ''    
    url = "{0}/topologies/{1}/tasks/".format(dpsBase, topic)
    authString = "{0}:{1}".format(login, password)
    
    response, code, headers = sendMessage(url, data=message, method='POST', auth=authString, proxy=proxyString)

    if code != 201 and code != 200:
        print("Problem with DPS task submitting. CODE:{0}".format(code))
        print(response.decode('utf-8'))
        return None
    else:
        return headers["Location"]
        

#-------------------------------
def prepareDpsMessage(parameters):
    ''
    taskName = parameters["taskName"]
    
    if taskName == "NewDataset":
        return dpsTask_dataset.format(parameters["providerId"], parameters["datasetId"], parameters["indexerName"], 
                               parameters["index"], parameters["type"] or "", parameters["extractText"], 
                               parameters["indexData"], parameters["storeExtracted"])
    elif taskName == "NewFile":
        return dpsTask_file.format(parameters["fileUrl"], parameters["indexerName"], 
                               parameters["index"], parameters["type"] or "", parameters["extractText"], 
                               parameters["indexData"], parameters["storeExtracted"])
 
     
#-------------------------------
if __name__ == '__main__':

    #--- Load params ---
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:", ["help", "config-file="])
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
    
    #send data to MCS
    if config["importConfigFile"]:
        print("\n##########  Data import is started  ##########\n")
        importData.start(["-c", config["importConfigFile"]])
        print("\n##########  Import is completed  ##########\n")
    
    #config Solr
    if config["solrHome"] and config["coreName"]:
        print("\n##########  Solr configuration is started  ##########\n")
        prepareSolr(config["solrHome"], config["coreName"])
        print("\n##########  Solr is configured  ##########\n")
    
    #config Elasticsearch
    if config["elasticsearchUrl"] and config["indexName"] and config["typeName"]:
        print("\n##########  Elasticsearch configuration is started  ##########\n")
        prepareElasticsearch(config["elasticsearchUrl"], config["indexName"], config["typeName"], config["proxy"] or "")
        print("\n##########  Elasticsearch is configured  ##########\n")
        
    #create topics
    if config["topics"] and config["kafkaHome"] and config["zookeeper"]:
        print("\n##########  Create topics are started  ##########\n")
        prepareKafkaTopics(config["kafkaHome"], config["zookeeper"], config["topics"])
        print("\n##########  Topics are created  ##########\n")
    
    #submit topologies
    if config["topologies"] and config["stormHome"]:
        print("\n##########  Submit topologies are started  ##########\n")
        for topology in config["topologies"]:  
            print("\n##########  Submitting {0} topology  ##########\n".format(topology["name"])) 
            if topology["jar"] and topology["mainClass"] and topology["name"]:
                submitTopology(config["stormHome"], topology["jar"], topology["mainClass"], 
                               topology["name"], topology["jvmParams"] or "")
        print("\n##########  Topologis are submited  ##########\n")
    
    #send DPS task
    if config["dps"] and config["task"]:
        print("\n##########  Processing of DPS task is started  ##########\n")
        login = config["task"]["login"]
        password = config["task"]["password"]
        
        #set permition
        setPermitionForTopology(config["dps"], config["task"]["topic"], login, password, config["proxy"] or "")
        print("\n##########  Permition is added  ##########\n")
        
        #send task
        message = prepareDpsMessage(config["task"])
        taskUrl = sendDpsTask(config["dps"], config["task"]["topic"], message, login, password, config["proxy"] or "")
        print("\n##########  DPS task is sent  ##########\n")
        if taskUrl:
            print("Submited task url: "+taskUrl)
    