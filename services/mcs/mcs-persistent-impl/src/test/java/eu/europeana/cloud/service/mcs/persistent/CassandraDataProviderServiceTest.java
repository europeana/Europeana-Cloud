package eu.europeana.cloud.service.mcs.persistent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.is;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

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
        assertTrue("Expecting no providers", cassandraDataProviderService.getProviders().isEmpty());
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

        Set<DataProvider> fetchedProviders = new HashSet<>(cassandraDataProviderService.getProviders());
        assertThat(fetchedProviders, is(insertedProviders));
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