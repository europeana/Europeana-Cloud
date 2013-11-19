package eu.europeana.cloud.service.mcs.persistent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.hamcrest.Matchers.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraDataProviderServiceTest extends CassandraTestBase {
	
	@Autowired
	private CassandraDataProviderService cassandraDataProviderService;
	
	
	@Test
	public void shouldCreateAndGetProvider() {
		DataProvider dp = cassandraDataProviderService.createProvider("provident", createRandomDataProviderProperties());
		
		DataProvider dpFromService = cassandraDataProviderService.getProvider("provident");
		assertThat(dp, is(dpFromService));
	}
	
	
	@Test(expected = ProviderNotExistsException.class)
	public void shouldDeleteProvider() {
		// given particular provider in service;
		DataProvider dp = cassandraDataProviderService.createProvider("provident", createRandomDataProviderProperties());

		// when this provider is deleted
		cassandraDataProviderService.deleteProvider(dp.getId());

		// then it should no more be available in service
		cassandraDataProviderService.getProvider(dp.getId());
	}
	
	
	@Test(expected = ProviderNotExistsException.class)
	public void shouldFailWhenFetchingNonExistingProvider() {
		cassandraDataProviderService.getProvider("provident");
	}
	
	
	@Test
	public void shouldReturnEmptyArrayWhenNoProviderAdded() {
		assertTrue("Expecting no providers", cassandraDataProviderService.getProviders(null, 1).getResults().isEmpty());
	}
	
	
	@Test
	public void shouldReturnAllProviders() {
		int providerCount = 10;
		Set<DataProvider> insertedProviders = new HashSet<>(providerCount * 2);

		// insert random providers
		for (int provId = 0; provId < providerCount; provId++) {
			insertedProviders.add(
					cassandraDataProviderService.createProvider("dp_" + provId, createRandomDataProviderProperties()));
		}
		
		Set<DataProvider> fetchedProviders = new HashSet<>(cassandraDataProviderService.getProviders(null, 100).
				getResults());
		assertThat(fetchedProviders, is(insertedProviders));
	}
	
	
	@Test
	public void shouldReturnPagedResults() {
		int providerCount = 1000;
		List<String> insertedProviderIds = new ArrayList<>(providerCount);

		// insert random providers
		for (int provId = 0; provId < providerCount; provId++) {
			DataProvider prov = cassandraDataProviderService.
					createProvider("dp_" + provId, createRandomDataProviderProperties());
			insertedProviderIds.add(prov.getId());
		}

		// iterate through all providers
		List<String> fetchedProviderIds = new ArrayList<>(providerCount);
		int sliceSize = 10;
		String token = null;
		do {
			ResultSlice<DataProvider> resultSlice = cassandraDataProviderService.getProviders(token, sliceSize);
			token = resultSlice.getNextSlice();
			assertTrue(resultSlice.getResults().size() == sliceSize || token == null);
			for (DataProvider dp: resultSlice.getResults()) {
				fetchedProviderIds.add(dp.getId());
			}
		} while (token != null);
		
		Collections.sort(insertedProviderIds);
		Collections.sort(fetchedProviderIds);
		assertThat(insertedProviderIds, is(fetchedProviderIds));
	}
	
	
	private DataProviderProperties createRandomDataProviderProperties() {
		DataProviderProperties properties = new DataProviderProperties();
		properties.setContactPerson("Contact_Person_" + randomString());
		properties.setDigitalLibraryURL("http://library.url/" + randomString());
		properties.setDigitalLibraryWebsite("http://library.url/website/" + randomString());
		properties.setOfficialAddress("Address/" + randomString());
		properties.setOrganisationName("Organisation_Name_" + randomString());
		properties.setOrganisationWebsite("http://organisation.url/" + randomString());
		properties.setOrganisationWebsiteURL("http://organisation.url/website" + randomString());
		properties.setRemarks("Important remarks for provider include " + randomString());
		return properties;
	}
	
	
	private static String randomString() {
		byte[] randomBytes = new byte[10];
		ThreadLocalRandom.current().nextBytes(randomBytes);
		return BaseEncoding.base64().encode(randomBytes);
	}
}
