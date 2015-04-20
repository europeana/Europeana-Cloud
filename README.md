Europeana Cloud
======

Europeana Cloud is a Best Practice Network, coordinated by [The European Library](http://www.theeuropeanlibrary.org/), designed to establish a cloud-based system for Europeana and its aggregators.

Lasting from 2013 to 2015, Europeana Cloud will provide new content, new metadata, a new linked storage system, new tools and services for researchers and a new platform - Europeana Research. Content providers and aggregators across the European information landscape urgently need a cheaper, more sustainable infrastructure that is capable of storing both metadata and content.

More information: http://pro.europeana.eu/web/europeana-cloud


## Infrastructure

The infrastructure of the project is developed jointly by [The Poznań Supercomputing and Networking Center](http://www.man.poznan.pl/online/en/), [The European Library](http://www.theeuropeanlibrary.org/), and [The Europeana Foundation](http://www.europeana.eu/). 

Deliverable 2.2 of the project provides technical specification of the infrastructure along with the requirement gathering process. The recent version of the deliverable is available [here](http://pro.europeana.eu/web/europeana-cloud/results/-/document_library_display/p6BV/view/1861920/57410).


## REST API pages
http://sonar.eanadev.org/job/oncommit-eCloud/UIS_REST_API/
http://sonar.eanadev.org/job/oncommit-eCloud/MCS_REST_API/

### REST API for 0.2ver
https://felicia.man.poznan.pl/docs/v0.2/uis/

https://felicia.man.poznan.pl/docs/v0.2/mcs/

https://felicia.man.poznan.pl/docs/v0.2/aas/

https://felicia.man.poznan.pl/docs/v0.2/dls/

# Usage

## Setting-up a provider

The `UISClient` must be initialized as the first step.

```java
UISClient uisClient = new UISClient("UIS-url", "username", "password");
```

### Creating a data provider


First create `DataProviderProperties` with information about some provider (name, website, etc).

```java
DataProviderProperties providerProperties = new DataProviderProperties("Sveriges nationalbibliotek", "address", "organisationWebsite", "organisationWebsiteURL", "digitalLibraryWebsite", "digitalLibraryURL", "contactPerson", "remarks");
```

Create the `Provider`

```java
uisClient.createProvider("providerId", providerProperties);
```

### Creating a cloud Id

 
```java
// "providerId"  created in the previous step)
CloudId cloudId = uisClient.createCloudId("providerId");
```

## Inserting a record

Both the `RecordServiceClient` and the `FileServiceClient` must first be initialized.

```java
RecordServiceClient recordClient = new RecordServiceClient("baseUrl", "username", "password");
```
```java
FileServiceClient fileClient = new FileServiceClient("baseUrl", "username", "password");
```

### Creating a representation  

It is assumed a `Provider` and a `CloudId` are already inserted in the system, as shown in the previous section.

```java
// CloudId created in previous step
URI reprUri = recordClient.createRepresentation(cloudId.getId(), "edm", "providerId");
```

On every call of `createRepresentation()` a new `Representation` is created (the old ones still remain alive). The representation can be accessed by the URI returned. 

For example:
```java
System.out.println(reprUri);
```

will return:
```java
http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8
```


Entering the URI in a browser will return the representation:
```xml
<representation>
  <allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/representations/edm/versions</allVersionsUri>
  <cloudId>L9WSPSMVQ85</cloudId>
  <creationDate>2014-11-20T17:11:32.193+01:00</creationDate>
  <dataProvider>provider</dataProvider>
  <persistent>false</persistent>
  <representationName>edm</representationName>
  <uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/representations/edm/versions/e4859b60-70cf-11e4-8fe1-00163eefc9c8</uri>
  <version>e4859b60-70cf-11e4-8fe1-00163eefc9c8</version>
</representation>
```



### Uploading a File  

```java
String myXml = "<myxml><someElement /></myxml>";
        
byte[] bytes = myXml.getBytes("UTF-8");
InputStream contentStream = new ByteArrayInputStream(bytes);
String mediaType = "text/xml";

URI myXmlUri = fileClient.uploadFile(cloudId.getId(), "edm", "b17c4f60-70d0-11e4-8fe1-00163eefc9c8", contentStream, mediaType);
```

On every call of `uploadFile()` a URI is returned that provides access to the uploaded content.

For example:
```java
System.out.println(myXmlUri);
```

will return 
```java
http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/ef9322a1-5416-4109-a727-2bdfecbf352d
```

However, accessing this URI from your browser is NOT possible, as the uploaded content is by default locked and not accessible to the public. 


### Accessing the uploaded File  

Accessing the uploaded content is possible by using the `FileServiceClient` with the same username + password used to create the content. For example:

```java
InputStream myInputStream = fileClient.getFile(cloudId.getId(), "edm", "b17c4f60-70d0-11e4-8fe1-00163eefc9c8", "ef9322a1-5416-4109-a727-2bdfecbf352d");
```

## Maven depedencies

For the `UISClient`:
```
<dependency>
    <groupId>eu.europeana.cloud</groupId>
    <artifactId>ecloud-service-uis-rest-client-java</artifactId>
    <version>0.3-SNAPSHOT</version>
</dependency>
```

For the `FileServiceClient` and `RecordServiceClient`:
```
<dependency>
    <groupId>eu.europeana.cloud</groupId>
    <artifactId>ecloud-service-mcs-rest-client-java</artifactId>
    <version>0.3-SNAPSHOT</version>
</dependency>
```

## eCloud test environment

The following details can be used to connect to an eCloud playground environment.

```java
String UIS_BASE_URL_ISTI = "http://ecloud.eanadev.org:8080/ecloud-service-uis-rest-0.3-SNAPSHOT";
String MCS_BASE_URL_ISTI = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";

UISClient uisClient = new UISClient(UIS_BASE_URL_ISTI, "Cristiano", "Ronaldo");
RecordServiceClient recordClient = new RecordServiceClient(MCS_BASE_URL_ISTI, "Cristiano", "Ronaldo");
FileServiceClient fileClient = new FileServiceClient(MCS_BASE_URL_ISTI, "Cristiano", "Ronaldo");
```



===+
