package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.*;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.UniqueIdentifierResource;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

/**
 * UniqueIdResource unit test
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 23, 2013
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SpringUserUtils.class)
public class UniqueIdentifierResourceTest extends JerseyTest {

    private UniqueIdentifierService uniqueIdentifierService;
    private DataProviderResource dataProviderResource;
    private MutableAclService mutableAclService;
    private ACLServiceWrapper aclWrapper;
    private String providerId = "providerId";
    private String recordId = "recordId";



    /**
     * Configuration of the Spring context
     */
    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(CloudIdDoesNotExistExceptionMapper.class)
                .registerClasses(CloudIdAlreadyExistExceptionMapper.class)
                .registerClasses(DatabaseConnectionExceptionMapper.class)
                .registerClasses(IdHasBeenMappedExceptionMapper.class)
                .registerClasses(ProviderDoesNotExistExceptionMapper.class)
                .registerClasses(RecordDatasetEmptyExceptionMapper.class)
                .registerClasses(RecordDoesNotExistExceptionMapper.class)
                .registerClasses(RecordExistsExceptionMapper.class)
                .registerClasses(RecordIdDoesNotExistExceptionMapper.class)
                .registerClasses(UniqueIdentifierResource.class)

                .property("contextConfigLocation", "classpath:uir-context-test.xml");
    }


    /**
     * Initialization of the Unique Identifier service mockup
     */
    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        uniqueIdentifierService = applicationContext.getBean(UniqueIdentifierService.class);
        dataProviderResource = applicationContext.getBean(DataProviderResource.class);
        mutableAclService = applicationContext.getBean(MutableAclService.class);
        aclWrapper = applicationContext.getBean(ACLServiceWrapper.class);
        Mockito.reset(uniqueIdentifierService);
        Mockito.reset(dataProviderResource);
    }


    /**
     * Test to create a cloud Id
     *
     * @throws Exception
     */
    @Test
    public void testCreateCloudId()
            throws Exception {
        CloudId originalGid = createCloudId(providerId, recordId);
        when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(originalGid);
        // Create a single object test
        Response response = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).post(null);
        assertThat(response.getStatus(), is(200));
        CloudId retrieveCreate = response.readEntity(CloudId.class);
        assertEquals(originalGid.getId(), retrieveCreate.getId());
        assertEquals(originalGid.getLocalId().getProviderId(), retrieveCreate.getLocalId().getProviderId());
        assertEquals(originalGid.getLocalId().getRecordId(), retrieveCreate.getLocalId().getRecordId());
    }


    @Test
    public void shouldTry4TimesAndFailInCaseOfExceptionWhileUpdatingACL()
            throws Exception {
        CloudId originalGid = createCloudId(providerId, recordId);
        when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(originalGid);
        // Create a single object test
        PowerMockito.mockStatic(SpringUserUtils.class);
        PowerMockito.when(SpringUserUtils.getUsername()).thenReturn("Ola");
        MutableAcl mutableAcl = mock(MutableAcl.class);
        when(mutableAclService.createAcl(any(ObjectIdentity.class))).thenReturn(mutableAcl);
        doThrow(Exception.class).when(mutableAclService).updateAcl(any(MutableAcl.class));

        Response response = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).
                        post(null);

        verify(mutableAclService, times(4)).updateAcl(any(MutableAcl.class));
        assertThat(response.getStatus(), is(500));
    }

    /**
     * Test the database exception
     *
     * @throws Exception
     */
    @Test
    public void testCreateCloudIdDbException()
            throws Exception {
        Throwable databaseException = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(databaseException);

        Response resp = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).post(null);
        assertThat(resp.getStatus(), is(500));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        StringUtils.equals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }


    /**
     * Test the a cloud id already exists for the record id
     *
     * @throws Exception
     */
    @Test
    public void testCreateCloudIdRecordExistsException()
            throws Exception {
        Throwable exception = new RecordExistsException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
                IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId)));

        when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(exception);

        Response resp = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).post(null);
        assertThat(resp.getStatus(), is(409));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId).getErrorCode());
        StringUtils.equals(errorInfo.getDetails(),
                IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId).getDetails());
    }


    /**
     * Test the a cloud id already exists for the record id
     *
     * @throws Exception
     */
    @Test
    public void testCreateCloudIdAlreadyExistException()
            throws Exception {
        // given
        Throwable exception = new CloudIdAlreadyExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getHttpCode(),
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(providerId, recordId)));

        when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(exception);
        // when
        Response resp = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).post(null);
        // when
        assertThat(resp.getStatus(), is(409));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
        StringUtils.equals(errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(providerId, recordId).getDetails());
    }


    /**
     * Test the retrieval of a cloud id
     *
     * @throws Exception
     */
    @Test
    public void testGetCloudId()
            throws Exception {
        CloudId originalGid = createCloudId(providerId, recordId);
        when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenReturn(originalGid);
        // Retrieve the single object by provider and recordId
        Response response = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request().get();

        assertThat(response.getStatus(), is(200));
        CloudId retrieveGet = response.readEntity(CloudId.class);
        assertEquals(originalGid.getId(), retrieveGet.getId());
        assertEquals(originalGid.getLocalId().getProviderId(), retrieveGet.getLocalId().getProviderId());
        assertEquals(originalGid.getLocalId().getRecordId(), retrieveGet.getLocalId().getRecordId());
    }


    /**
     * Test the database exception
     *
     * @throws Exception
     */
    @Test
    public void testGetCloudIdDBException()
            throws Exception {
        Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenThrow(exception);

        Response resp = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
        assertThat(resp.getStatus(), is(500));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        StringUtils.equals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }


    /**
     * Test the exception that a gloabl id does not exist for this record id
     *
     * @throws Exception
     */
    @Test
    public void testGetCloudIdRecordDoesNotExistException()
            throws Exception {
        Throwable exception = new RecordDoesNotExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getHttpCode(),
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId)));

        when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenThrow(exception);

        Response resp = target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
        assertThat(resp.getStatus(), is(404));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
        StringUtils.equals(errorInfo.getDetails(),
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getDetails());
    }


    /**
     * Test the retrieval of local ids by a cloud id
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testGetLocalIds()
            throws Exception {
        ResultSlice<CloudId> lidListWrapper = new ResultSlice<>();
        List<CloudId> localIdList = new ArrayList<>(1);
        localIdList.add(createCloudId(providerId, recordId));
        String cloudId = createCloudId(providerId, recordId).getId();
        lidListWrapper.setResults(localIdList);
        when(uniqueIdentifierService.getLocalIdsByCloudId(cloudId)).thenReturn(localIdList);
        Response response = target("/cloudIds/" + cloudId).request().get();
        assertThat(response.getStatus(), is(200));
        ResultSlice<CloudId> retList = response.readEntity(ResultSlice.class);
        assertThat(retList.getResults().size(), is(lidListWrapper.getResults().size()));
        assertEquals(retList.getResults().get(0).getLocalId().getProviderId(), lidListWrapper.getResults().get(0)
                .getLocalId().getProviderId());
        assertEquals(retList.getResults().get(0).getLocalId().getRecordId(), lidListWrapper.getResults().get(0)
                .getLocalId().getRecordId());
    }


    /**
     * Test the database exception
     *
     * @throws Exception
     */
    @Test
    public void testGetLocalIdsDBException()
            throws Exception {
        Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        when(uniqueIdentifierService.getLocalIdsByCloudId("cloudId")).thenThrow(exception);

        Response resp = target("/cloudIds/cloudId").request().get();
        assertThat(resp.getStatus(), is(500));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        StringUtils.equals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }


    /**
     * Test that a cloud id does not exist
     *
     * @throws Exception
     */
    @Test
    public void testGetLocalIdsCloudIdDoesNotExistException()
            throws Exception {
        Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

        when(uniqueIdentifierService.getLocalIdsByCloudId("cloudId")).thenThrow(exception);

        Response resp = target("/cloudIds/cloudId").request().get();
        assertThat(resp.getStatus(), is(404));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
        StringUtils.equals(errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getDetails());
    }


    /**
     * Test the deletion of a cloud id
     *
     * @throws Exception
     */
    @Test
    public void testDeleteCloudId()
            throws Exception {
        CloudId gid = createCloudId(providerId, recordId);
        when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(gid);
        target("/cloudIds").queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).post(null);
        Response res = target("/cloudIds/" + gid.getId()).request().delete();
        assertThat(res.getStatus(), is(200));
    }


    /**
     * Test the database exception
     *
     * @throws Exception
     */
    @Test
    public void testDeleteCloudIdDBException()
            throws Exception {
        Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

        Response resp = target("/cloudIds/cloudId").request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML)
                .delete();
        assertThat(resp.getStatus(), is(500));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        StringUtils.equals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }


    /**
     * Test the exception of the removal of a cloud id when a cloud id does not
     * exist
     *
     * @throws Exception
     */
    @Test
    public void testDeleteCloudIdCloudIdDoesNotExistException()
            throws Exception {
        Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

        doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

        Response resp = target("/cloudIds/cloudId").request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML)
                .delete();
        assertThat(resp.getStatus(), is(404));
        ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
        StringUtils.equals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
        StringUtils.equals(errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getDetails());
    }


    private static LocalId createLocalId(String providerId, String recordId) {
        LocalId localId = new LocalId();
        localId.setProviderId(providerId);
        localId.setRecordId(recordId);
        return localId;
    }


    private static CloudId createCloudId(String providerId, String recordId) {
        CloudId cloudId = new CloudId();
        cloudId.setLocalId(createLocalId(providerId, recordId));
        cloudId.setId(IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + recordId));
        return cloudId;
    }
}
