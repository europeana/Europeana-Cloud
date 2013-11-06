package eu.europeana.cloud.service.mcs.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * FileResourceTest
 */
public class RepresentationSearchTest extends JerseyTest {

    private static RecordService recordService;

    private WebTarget representationSearchWebTarget;

    private Representation s1_p1;

    private Representation s1_p2;

    private Representation s2_p1;


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        representationSearchWebTarget = target(RepresentationSearchResource.class.getAnnotation(Path.class).value());

        s1_p1 = recordService.createRepresentation("cloud_1", "s1", "p1");
        s1_p2 = recordService.createRepresentation("cloud_2", "s1", "p2");
        s2_p1 = recordService.createRepresentation("cloud_3", "s2", "p1");
    }


    @After
    public void cleanUp() {
        recordService.deleteRecord("cloud_1");
        recordService.deleteRecord("cloud_2");
        recordService.deleteRecord("cloud_3");
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Test
    public void shouldSearchForSchema()
            throws IOException {
        // given representations s1_p1, s1_p2, s2_p1

        // when searching for schema s1
        Response searchForSchemaResponse = representationSearchWebTarget.queryParam(ParamConstants.F_REP, "s1").request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(), searchForSchemaResponse.getStatus());
        List<Representation> s1Representations = searchForSchemaResponse.readEntity(new GenericType<List<Representation>>() {
        });

        // then received representations should be all from s1 schema
        assertEquals(new HashSet<>(Arrays.asList(s1_p1, s1_p2)), new HashSet<>(s1Representations));
    }


    @Test
    public void shouldSearchForProvider()
            throws IOException {
        // given representations s1_p1, s1_p2, s2_p1

        // when searching for provider p1
        Response searchForProviderResponse = representationSearchWebTarget.queryParam(ParamConstants.F_PROVIDER, "p1").request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(), searchForProviderResponse.getStatus());
        List<Representation> p1Representations = searchForProviderResponse.readEntity(new GenericType<List<Representation>>() {
        });

        // then received representations should be all from p1 provider
        assertEquals(new HashSet<>(Arrays.asList(s1_p1, s2_p1)), new HashSet<>(p1Representations));
    }


    @Test
    public void shouldSearchForProviderAndSchema()
            throws IOException {
        // given representations s1_p1, s1_p2, s2_p1

        // when searching for provider p1
        Response searchResponse = representationSearchWebTarget.queryParam(ParamConstants.F_PROVIDER, "p1")
                .queryParam(ParamConstants.F_REP, "s1").request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(), searchResponse.getStatus());
        List<Representation> foundRepresenations = searchResponse.readEntity(new GenericType<List<Representation>>() {
        });

        // then received representations should be all from p1 provider s1 schema
        assertEquals(new HashSet<>(Arrays.asList(s1_p1)), new HashSet<>(foundRepresenations));
    }
}
