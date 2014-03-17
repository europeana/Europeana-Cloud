package eu.europeana.cloud.service.dls.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;

import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.ApplicationContextUtils;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

/**
 * FileResourceTest
 */
@RunWith(JUnitParamsRunner.class)
public class RepresentationSearchTest extends JerseyTest {

    private WebTarget representationSearchWebTarget;

    private SolrDAO solrDAO;

    private Representation s1_p1;

    private Representation s1_p2;

    private Representation s2_p1;


    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        representationSearchWebTarget = target(RepresentationSearchResource.class.getAnnotation(Path.class).value());
        solrDAO = applicationContext.getBean(SolrDAO.class);

        CompoundDataSetId dataSetId_p1 = new CompoundDataSetId("p1", "ds");
        CompoundDataSetId dataSetId_p2 = new CompoundDataSetId("p2", "ds");
        s1_p1 = getDummyRepresentation("cloud_1", "s1", "p1");
        s1_p2 = getDummyRepresentation("cloud_2", "s1", "p2");
        s2_p1 = getDummyRepresentation("cloud_3", "s2", "p1");

        solrDAO.insertRepresentation(s1_p1, Collections.singleton(dataSetId_p1));
        solrDAO.insertRepresentation(s1_p2, Collections.singleton(dataSetId_p2));
        solrDAO.insertRepresentation(s2_p1, Collections.singleton(dataSetId_p1));
    }


    private Representation getDummyRepresentation(String cloudId, String representationName, String provider) {
        Representation rep = new Representation();
        rep.setCloudId(cloudId);
        rep.setRepresentationName(representationName);
        rep.setDataProvider(provider);
        rep.setVersion(UUID.randomUUID().toString());
        rep.setCreationDate(new Date());
        return rep;
    }


    @After
    public void cleanUp()
            throws Exception {
        solrDAO.removeRepresentationVersion(s1_p1.getVersion());
        solrDAO.removeRepresentationVersion(s1_p2.getVersion());
        solrDAO.removeRepresentationVersion(s2_p1.getVersion());
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:restServicesTestContext.xml");
    }


    @Test
    public void shouldSearchForSchema()
            throws IOException {
        // given representations s1_p1, s1_p2, s2_p1

        // when searching for schema s1
        Response searchForSchemaResponse = representationSearchWebTarget
                .queryParam(ParamConstants.F_REPRESENTATIONNAME, "s1").request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(), searchForSchemaResponse.getStatus());
        List<Representation> s1Representations = searchForSchemaResponse.readEntity(ResultSlice.class).getResults();

        // then received representations should be all from s1 schema
        assertEquals(new HashSet<>(Arrays.asList(s1_p1, s1_p2)), new HashSet<>(s1Representations));
    }


    @Test
    public void shouldSearchForProvider()
            throws IOException {
        // given representations s1_p1, s1_p2, s2_p1

        // when searching for provider p1
        Response searchForProviderResponse = representationSearchWebTarget.queryParam(ParamConstants.F_PROVIDER, "p1")
                .request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(),
            searchForProviderResponse.getStatus());
        List<Representation> p1Representations = searchForProviderResponse.readEntity(ResultSlice.class).getResults();

        // then received representations should be all from p1 provider
        assertEquals(new HashSet<>(Arrays.asList(s1_p1, s2_p1)), new HashSet<>(p1Representations));
    }


    @Test
    public void shouldSearchForProviderAndSchema()
            throws IOException {
        // given representations s1_p1, s1_p2, s2_p1

        // when searching for provider p1
        Response searchResponse = representationSearchWebTarget.queryParam(ParamConstants.F_PROVIDER, "p1")
                .queryParam(ParamConstants.F_REPRESENTATIONNAME, "s1").request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(), searchResponse.getStatus());
        List<Representation> foundRepresenations = searchResponse.readEntity(ResultSlice.class).getResults();

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
    public void shouldParseIsoDatesInSearch() {
        representationSearchWebTarget = representationSearchWebTarget.queryParam(ParamConstants.F_DATE_FROM,
            "1995-12-31T23:59:59.999Z").queryParam(ParamConstants.F_DATE_UNTIL, "2004-02-12T15:19:21+02:00");
        Response searchResponse = representationSearchWebTarget.request().get();
        assertEquals("Unexpected status code ", Response.Status.OK.getStatusCode(), searchResponse.getStatus());
    }


    @Test
    public void shouldFailIfNoIsoDatesInSearch() {
        representationSearchWebTarget = representationSearchWebTarget.queryParam(ParamConstants.F_DATE_FROM,
            "31-12-1995");
        Response searchResponse = representationSearchWebTarget.request().get();
        assertEquals("Unexpected status code ", Response.Status.BAD_REQUEST.getStatusCode(), searchResponse.getStatus());
    }


    @SuppressWarnings("unused")
    private List<Map<String, String>> searchParams() {
        Map<String, String> allQueryParams = new HashMap<>();
        allQueryParams.put(ParamConstants.F_REPRESENTATIONNAME, "s1");
        allQueryParams.put(ParamConstants.F_PROVIDER, "p1");
        allQueryParams.put(ParamConstants.F_DATE_FROM, "1995-12-31T23:59:59.999Z");
        allQueryParams.put(ParamConstants.F_PERSISTENT, "TRUE");
        allQueryParams.put(ParamConstants.F_DATE_UNTIL, "2004-02-12T15:19:21+02:00");
        allQueryParams.put(ParamConstants.F_DATASET, "ds");

        // all possible param configurations
        Set<Set<String>> paramSubsets = Sets.powerSet(allQueryParams.keySet());
        List<Map<String, String>> allParamConfigs = new ArrayList<>(paramSubsets.size());
        for (Set<String> paramSubset : paramSubsets) {
            // param set must not be empty
            if (paramSubset.isEmpty()) {
                continue;
            }

            allParamConfigs.add(Maps.asMap(paramSubset, Functions.forMap(allQueryParams)));
        }
        return allParamConfigs;
    }


    @Test
    @Ignore("Too long and too unimportant")
    @Parameters(method = "searchParams")
    public void shouldSearchForAllParamConfigurations(Map<String, String> searchParams) {
        for (Map.Entry<String, String> param : searchParams.entrySet()) {
            representationSearchWebTarget = representationSearchWebTarget.queryParam(param.getKey(), param.getValue());
        }

        Response searchResponse = representationSearchWebTarget.request().get();
        assertEquals("Unexpected status code for params " + searchParams, Response.Status.OK.getStatusCode(),
            searchResponse.getStatus());
        List<Representation> foundRepresenations = searchResponse.readEntity(ResultSlice.class).getResults();

        assertFalse(foundRepresenations.isEmpty());
    }
}
