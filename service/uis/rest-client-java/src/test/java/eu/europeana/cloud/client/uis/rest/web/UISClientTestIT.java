package eu.europeana.cloud.client.uis.rest.web;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ResultSlice;
import net.iharder.Base64;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Ignore
public class UISClientTestIT {

    private static final String UIS_LOCATION = "http://127.0.0.1:8080/uis";
    private static final String USER = "testUser1";
    private static final String PASSWORD = "testUserPassword2";

    @Test
    public void shouldCreateProvider()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        DataProviderProperties dataProviderProperties =
                new DataProviderProperties(
                        "organizationName",
                        "officialAdress",
                        "organizationWebsite",
                        "websiteURL",
                        "libraryWebsite",
                        "libraryUrl",
                        "contactPerson",
                        "remarks");

        c.createProvider("providerId", dataProviderProperties);
    }

    @Test
    public void shouldUpdateProvider()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        DataProviderProperties dataProviderProperties =
                new DataProviderProperties(
                        "organizationName1",
                        "officialAdress1",
                        "organizationWebsite1",
                        "websiteURL1",
                        "libraryWebsite1",
                        "libraryUrl1",
                        "contactPerson1",
                        "remarks1");
        boolean result = c.updateProvider("providerId", dataProviderProperties);
        assertThat(result, is(true));
    }

    @Test
    public void shouldGetDataProviders()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        ResultSlice<DataProvider> providers = c.getDataProviders("");
    }

    @Test
    public void shouldGetDataProvider()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        DataProvider provider = c.getDataProvider("providerId");
    }

    @Test
    public void shouldCreateCloudId()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        CloudId createdCloudId = c.createCloudId("providerId", "recordId2");
        assertThat(createdCloudId.getLocalId().getRecordId(), is("recordId2"));
    }

    @Test
    public void shouldCreateCloudIdWithProvidedAuthHeader()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION);
        String userPasswordToken = USER + ":" + PASSWORD;
        String authHeader = "Basic " + Base64.encodeBytes(userPasswordToken.getBytes());
        CloudId createdCloudId = c.createCloudId("providerId", "recordId3", "Authorization", authHeader);
        assertThat(createdCloudId.getLocalId().getRecordId(), is("recordId3"));
    }

    @Test
    public void shouldCreateCloudIdForProviderIdValueOnlyProvided() throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION);

        CloudId createdCloudId = c.createCloudId("providerId");
        assertThat(createdCloudId.getLocalId().getProviderId(), is("providerId"));
    }

    @Test
    public void shouldGetCloudId()
            throws CloudException {
        try {
            UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
            CloudId cloudId = c.getCloudId("providerId", "recordId");
            assertThat(cloudId.getLocalId().getRecordId(), is("recordId"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldGetCloudIdWithProvidedAuthHeader() {

        try {
            String userPasswordToken = USER + ":" + PASSWORD;
            String sample = "Basic " + Base64.encodeBytes(userPasswordToken.getBytes());

            UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
            CloudId cloudId = c.getCloudId("providerId", "recordId", "Authorization", sample);
            assertThat(cloudId.getLocalId().getRecordId(), is("recordId"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldGetRecordId()
            throws CloudException {

        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        ResultSlice<CloudId> cloudIds = c.getRecordId("AD2A6DWBPNSUSIM5FXFW7RXFJKI4LY3BZJGZ336XFPVCHX6G2HKA");
        cloudIds.toString();
    }

    @Test
    public void shouldGetRecordIdsByProvider()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        ResultSlice<CloudId> cloudIds = c.getCloudIdsByProvider("providerId");
    }

    @Test
    public void shouldGetCloudIdsByProviderWithPagination()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        ResultSlice<CloudId> cloudIds = c.getCloudIdsByProviderWithPagination("providerId", "RK", 100);
    }

    @Test
    public void shouldGetRecordIdsByProviderWithPagination()
            throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        ResultSlice<LocalId> cloudIds = c.getRecordIdsByProviderWithPagination("providerId", "1", 100);
    }

    @Test
    public void shouldCreateMapping() throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        boolean result = c.createMapping("AD2A6DWBPNSUSIM5FXFW7RXFJKI4LY3BZJGZ336XFPVCHX6G2HKA", "providerId", "startRecordId");
    }

    @Test
    public void shouldCreateMappingWithProvidedAuthHeader() throws CloudException {
        String userPasswordToken = USER + ":" + PASSWORD;
        String authHeader = "Basic " + Base64.encodeBytes(userPasswordToken.getBytes());

        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        boolean result = c.createMapping("AD2A6DWBPNSUSIM5FXFW7RXFJKI4LY3BZJGZ336XFPVCHX6G2HKA", "providerId", "startRecordId1", "Authorization", authHeader);
    }

    @Test
    public void shouldRemoveMappingByLocalId()
            throws CloudException {

        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        boolean result = c.removeMappingByLocalId("providerId", "startRecordId1");
    }

    @Test
    public void shouldDeleteCloudId() throws CloudException {
        UISClient c = new UISClient(UIS_LOCATION, USER, PASSWORD);
        boolean result = c.deleteCloudId("FN4JFCFKTZNLC4YZRLRIMOKJBMH37U55XDTA5ZWUGWOICO6S5PNQ");
    }
}
