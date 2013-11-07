package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;


import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * FileResourceTest
 */
@RunWith(JUnitParamsRunner.class)
public class RepresentationSearchTest extends JerseyTest {

    private RecordService recordService;

    private DataProviderService providerService;

    private DataSetService dataSetService;

    private WebTarget representationSearchWebTarget;

    private Representation s1_p1;

    private Representation s1_p2;

    private Representation s2_p1;


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        representationSearchWebTarget = target(RepresentationSearchResource.class.getAnnotation(Path.class).value());
        providerService = applicationContext.getBean(DataProviderService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);


        providerService.createProvider("p1", new DataProviderProperties());
        providerService.createProvider("p2", new DataProviderProperties());
        dataSetService.createDataSet("p1", "ds", "descr");

        s1_p1 = recordService.createRepresentation("cloud_1", "s1", "p1");
        s1_p2 = recordService.createRepresentation("cloud_2", "s1", "p2");
        s2_p1 = recordService.createRepresentation("cloud_3", "s2", "p1");

        dataSetService.addAssignment("p1", "ds", s1_p1.getRecordId(), s1_p1.getSchema(), s1_p1.getVersion());
        dataSetService.addAssignment("p1", "ds", s1_p2.getRecordId(), s1_p2.getSchema(), s1_p2.getVersion());
        dataSetService.addAssignment("p1", "ds", s2_p1.getRecordId(), s2_p1.getSchema(), s2_p1.getVersion());
    }


    @After
    public void cleanUp() {
        dataSetService.deleteDataSet("p1", "ds");
        providerService.deleteProvider("p1");
        providerService.deleteProvider("p2");

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


    @Test
    public void shouldNotSearchForEmptyParameters()
            throws IOException {
        Response searchResponse = representationSearchWebTarget.request().get();
        assertEquals("Unexpected status code", Response.Status.BAD_REQUEST.getStatusCode(), searchResponse.getStatus());
    }


    @Test
    public void shouldNotSearchWhenDataSetIsWithoutProvider()
            throws IOException {
        Response searchResponse = representationSearchWebTarget.queryParam(ParamConstants.F_DATASET, "ds").request().get();
        assertEquals("Unexpected status code", Response.Status.BAD_REQUEST.getStatusCode(), searchResponse.getStatus());

        searchResponse = representationSearchWebTarget.queryParam(ParamConstants.F_DATASET, "p1")
                .queryParam(ParamConstants.F_REP, "s1").request().get();
        assertEquals("Unexpected status code", Response.Status.BAD_REQUEST.getStatusCode(), searchResponse.getStatus());
    }


    @SuppressWarnings("unused")
    private List<Map<String, String>> searchParams() {
        Map<String, String> allQueryParams = ImmutableMap.of(
                ParamConstants.F_REP, "s1",
                ParamConstants.F_PROVIDER, "p1",
                ParamConstants.F_DATASET, "ds");
        // all possible param configurations
        Set<Set<String>> paramSubsets = Sets.powerSet(allQueryParams.keySet());
        List<Map<String, String>> allParamConfigs = new ArrayList<>(paramSubsets.size());
        for (Set<String> paramSubset : paramSubsets) {
            // param set must not be empty
            if (paramSubset.isEmpty()) {
                continue;
            }
            // param set containing data set without provider is wrong - it must be ignored;
            if (paramSubset.contains(ParamConstants.F_DATASET) && !paramSubset.contains(ParamConstants.F_PROVIDER)) {
                continue;
            }
            allParamConfigs.add(Maps.asMap(paramSubset, Functions.forMap(allQueryParams)));
        }
        return allParamConfigs;
    }


    @Test
    @Parameters(method = "searchParams")
    public void shouldSearchForAllParamConfigurations(Map<String, String> searchParams) {
        for (Map.Entry<String, String> param : searchParams.entrySet()) {
            representationSearchWebTarget = representationSearchWebTarget.queryParam(param.getKey(), param.getValue());
        }

        Response searchResponse = representationSearchWebTarget.request().get();
        assertEquals("Unexpected status code for params " + searchParams, Response.Status.OK.getStatusCode(), searchResponse.getStatus());
        List<Representation> foundRepresenations = searchResponse.readEntity(new GenericType<List<Representation>>() {
        });

        assertFalse(foundRepresenations.isEmpty());
    }
}
