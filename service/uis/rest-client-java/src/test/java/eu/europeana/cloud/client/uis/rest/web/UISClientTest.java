package eu.europeana.cloud.client.uis.rest.web;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ResultSlice;

/** 
 * Tests the UISClient, using Betamax. 
 * 
 * The rest API should be running in the URL defined below {@link UISClientTest#BASE_URL}
 * 
 * emmanouil.koufakis@theeuropeanlibrary.org
 */
public class UISClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    /** Needed to record the tests. */
    private static final String BASE_URL = "http://localhost:8081/ecloud-service-uis-rest";
    
    private static final String PROVIDER_ID = "TEST_PROVIDER";
    
    private static final String RECORD_ID = "TEST_RECORD";
    
    /**
     * Tests for:
     * 
     * Create, Retrieve provider.
     */
    @Betamax(tape = "UISClient/createAndRetrieveProviderTest")
    @Test
    public final void createAndRetrieveProviderTest() throws Exception {

    	final String providerId = "createAndRetrieveProviderTest_Id";
    	
        UISClient uisClient = new UISClient(BASE_URL);
    	
    	// Create some test properties and store them in the cloud
        DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        		"url", "url", "url", "person", "remarks");
        uisClient.createProvider(providerId, providerProperties);
        
        // Get back the properties and make sure they are the same
        DataProvider provider = uisClient.getDataProvider(providerId);
        assertNotNull(provider);
        assertNotNull(provider.getProperties());
        assertTrue(provider.getProperties().equals(providerProperties));
    }
    
    /**
     * 1) Creates some test properties and stores them in the cloud =>
     * 2) Makes a call to Update the provider with new properties =>
     * 3) Retrieves the properties and makes sure the updated properties come back.
     */
    @Betamax(tape = "UISClient/updateProvider")
    @Test
    public final void updateProviderTest() throws CloudException {
    	
    	final String providerId = "updateProviderTest_Id";
    	
        UISClient uisClient = new UISClient(BASE_URL);
    	
    	// Create some test properties and store them in the cloud
        DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        		"url", "url", "url", "person", "remarks");
        uisClient.createProvider(providerId, providerProperties);
        
        // Make a call to Update the provider with new properties 
        DataProviderProperties providerPropertiesUpdated = new DataProviderProperties("Name2", "Address2", "website2",
        		"url2", "url2", "url2", "person2", "remarks2");
        uisClient.updateProvider(providerId, providerPropertiesUpdated);

        // Retrieve the properties and make sure you get the updated properties back
        DataProvider providerUpdated = uisClient.getDataProvider(providerId);
        assertNotNull(providerUpdated);
        assertNotNull(providerUpdated.getProperties());
        assertTrue(providerUpdated.getProperties().equals(providerPropertiesUpdated));
    }
    
    @Betamax(tape = "UISClient/duplicateProvider")
    @Test
    public final void duplicateProviderRecordTest() throws Exception {
    	
        UISClient uisClient = new UISClient(BASE_URL);
    	
    	final String providerId = "Test provider Id";

    	// try to insert the same (PROVIDER_ID + RECORD_ID) twice
    	uisClient.createCloudId(providerId, RECORD_ID);
    	try{
    	uisClient.createCloudId(providerId, RECORD_ID);
    	} catch(Exception e) {
    		assertTrue(e instanceof CloudException);
    	}
    }
    
    @Betamax(tape = "UISClient/createMappingTest")
    @Test
    public final void createMappingTest() throws Exception {

        UISClient uisClient = new UISClient(BASE_URL);

        // create a mapping
        final CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);
        final LocalId localId = cloudId.getLocalId(); // local Id was created automatically
        assertTrue(localId != null);
        
        // lets test that we can get some results back using the previously created CloudId  
        ResultSlice<CloudId> resultSliceWithResults = uisClient.getRecordId(cloudId.getId());
        assertTrue(!resultSliceWithResults.getResults().isEmpty());
    }
    
    @Betamax(tape = "UISClient/removeMappingTest")
    @Test
    public final void removeMappingTest() throws Exception {
    	
        UISClient uisClient = new UISClient(BASE_URL);

        // create a mapping
        final CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);
        final LocalId localId = cloudId.getLocalId(); // local Id was created automatically
        
        // remove the mapping
        final boolean removedSuccesfully = uisClient.removeMappingByLocalId(localId.getProviderId(), localId.getRecordId());
        assertTrue(removedSuccesfully);
        
        // mapping was removed => we should get no results
        ResultSlice<CloudId> resultSliceNoResults = uisClient.getRecordId(cloudId.getId());
        assertTrue(resultSliceNoResults.getResults().isEmpty());
    }
    

    @Betamax(tape = "UISClient/removeAndRecreateMappingTest")
    @Test
    public final void removeAndRecreateMappingTest() throws Exception {
    	
        UISClient uisClient = new UISClient(BASE_URL);

        // create a mapping
        final CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);
        final LocalId localId = cloudId.getLocalId(); // local Id was created automatically
        
        // remove the mapping
        final boolean removedSuccesfully = uisClient.removeMappingByLocalId(localId.getProviderId(), localId.getRecordId());
        assertTrue(removedSuccesfully);
        
        // lets create the mapping again
        uisClient.createMapping(cloudId.getId(), localId.getProviderId(), localId.getRecordId());
        
        // lets test that we can get some results back
        ResultSlice<CloudId> resultSliceWithResults = uisClient.getRecordId(cloudId.getId());
        assertTrue(!resultSliceWithResults.getResults().isEmpty());
    }

    @Betamax(tape = "UISClient/createAndRetrieveRecordTest")
    @Test
    public final void createAndRetrieveRecordTest() throws Exception {

        UISClient uisClient = new UISClient(BASE_URL);
        
    	final String providerId = "createAndRetrieveRecordTestID";
    	
    	// create a record
        final CloudId cloudIdIhave = uisClient.createCloudId(providerId, RECORD_ID);
        ResultSlice<CloudId> resultsSlice = uisClient.getRecordId(cloudIdIhave.getId());
        
        // get back the record
        CloudId cloudIdIGotBack = resultsSlice.getResults().iterator().next();
        assertTrue(cloudIdIhave.equals(cloudIdIGotBack));
    }

    @Betamax(tape = "UISClient/deleteRecordTest")
    @Test(expected = CloudException.class)
    public final void deleteRecordTest() throws Exception {

        UISClient uisClient = new UISClient(BASE_URL);
    	
    	// create a record
        final CloudId cloudIdIhave = uisClient.createCloudId(PROVIDER_ID, RECORD_ID);
        
        // delete it
        final boolean isDeleted = uisClient.deleteCloudId(cloudIdIhave.getId());
        assertTrue(isDeleted);

        // try to get it back => should throw CloudIdDoesNotExist exception
        uisClient.getRecordId(cloudIdIhave.getId());
    }

    @Betamax(tape = "UISClient/createCloudIdandRetrieveCloudIdTest")
    @Test
    public final void createCloudIdandRetrieveCloudIdTest() throws Exception {
	  
    	UISClient uisClient = new UISClient(BASE_URL);
  	
    	final CloudId cloudIdIhave = uisClient.createCloudId(PROVIDER_ID, RECORD_ID);
    	ResultSlice<CloudId> resultsSlice = uisClient.getRecordId(cloudIdIhave.getId());
      
    	CloudId cloudIdIGotBack = resultsSlice.getResults().iterator().next();
    	assertTrue(cloudIdIhave.equals(cloudIdIGotBack));
    }    
}
