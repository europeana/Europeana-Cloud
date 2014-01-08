package eu.europeana.cloud.service.mcs.persistent.solr;

import eu.europeana.cloud.service.mcs.persistent.util.CompoundDataSetId;
import eu.europeana.cloud.service.mcs.persistent.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.persistent.solr.SolrConnectionProvider;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RepresentationSearchParams;
import eu.europeana.cloud.service.mcs.persistent.TestUtil;
import eu.europeana.cloud.service.mcs.persistent.exception.SolrDocumentNotFoundException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/solrTestContext.xml" })
public class SolrDAOSearchTest {

    @Autowired
    private SolrDAO solrDAO;

    @Autowired
    private SolrConnectionProvider connectionProvider;


    @After
    public void deleteData()
            throws IOException, SolrServerException {
        connectionProvider.getSolrServer().deleteByQuery("*:*");
        connectionProvider.getSolrServer().commit();
    }


    @Test
    public void searchBySchema()
            throws IOException, SolrServerException {
        Representation r1 = insertRepresentation("c1", "dc", "v1", "dp", true, new Date());
        Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
        Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
        Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp", true, new Date());

        List<Representation> foundRepresentations = solrDAO.search(RepresentationSearchParams.builder().setSchema("dc")
                .build(), 0, 10);
        TestUtil.assertSameContent(Arrays.asList(r1, r2), foundRepresentations);
    }


    @Test
    public void searchByDataSet()
            throws IOException, SolrServerException, SolrDocumentNotFoundException {
        // insert some representations
        Representation r1 = insertRepresentation("c1", "dc", "v1", "dp", true, new Date());
        Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
        Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
        Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp", true, new Date());

        // assign them to different sets
        solrDAO.addAssignment("v1", new CompoundDataSetId("dataSetProvider", "DS1"));
        solrDAO.addAssignment("v2", new CompoundDataSetId("dataSetProvider", "DS2"));
        solrDAO.addAssignment("v3", new CompoundDataSetId("anotherDataSetProvider", "DS1"));
        solrDAO.addAssignment("v4", new CompoundDataSetId("anotherDataSet", "ProviderDS1"));

        // search by data set provider and data set id
        TestUtil.assertSameContent(
            Arrays.asList(r1),
            solrDAO.search(
                RepresentationSearchParams.builder().setDataSetProviderId("dataSetProvider").setDataSetId("DS1")
                        .build(), 0, 10));

        // search by data set provider
        TestUtil.assertSameContent(Arrays.asList(r1, r2),
            solrDAO.search(RepresentationSearchParams.builder().setDataSetProviderId("dataSetProvider").build(), 0, 10));

        // search by data set id
        TestUtil.assertSameContent(Arrays.asList(r1, r3),
            solrDAO.search(RepresentationSearchParams.builder().setDataSetId("DS1").build(), 0, 10));
    }


    @Test
    public void searchByProvider()
            throws IOException, SolrServerException {
        Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, new Date());
        Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
        Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
        Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp1", true, new Date());

        List<Representation> foundRepresentations = solrDAO.search(RepresentationSearchParams.builder()
                .setDataProvider("dp1").build(), 0, 10);
        TestUtil.assertSameContent(Arrays.asList(r1, r4), foundRepresentations);
    }


    @Test
    public void searchBySchemaAndProvider()
            throws Exception {
        Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, new Date());
        Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
        Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
        Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp1", true, new Date());

        List<Representation> foundRepresentations = solrDAO.search(RepresentationSearchParams.builder()
                .setDataProvider("dp1").setSchema("dc").build(), 0, 10);
        TestUtil.assertSameContent(Arrays.asList(r1), foundRepresentations);
    }


    @Test
    public void searchByPersistent()
            throws Exception {
        Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, new Date());
        Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", false, new Date());

        List<Representation> onlyPersistent = solrDAO.search(RepresentationSearchParams.builder().setSchema("dc")
                .setPersistent(Boolean.TRUE).build(), 0, 10);
        TestUtil.assertSameContent(Arrays.asList(r1), onlyPersistent);
        List<Representation> onlyNotPersistent = solrDAO.search(RepresentationSearchParams.builder().setSchema("dc")
                .setPersistent(Boolean.FALSE).build(), 0, 10);
        TestUtil.assertSameContent(Arrays.asList(r2), onlyNotPersistent);
        List<Representation> regardlessPersistence = solrDAO.search(RepresentationSearchParams.builder()
                .setSchema("dc").build(), 0, 10);
        TestUtil.assertSameContent(Arrays.asList(r1, r2), regardlessPersistence);

    }


    @Test
    public void searchByDate()
            throws IOException, SolrServerException {
        Calendar c = GregorianCalendar.getInstance();
        c.set(2000, 05, 15, 15, 15);
        Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, c.getTime());
        c.set(2000, 05, 15, 15, 17);
        Representation r2 = insertRepresentation("c1", "dc", "v2", "dp1", true, c.getTime());
        c.set(2001, 05, 15, 15, 0);
        Representation r3 = insertRepresentation("c1", "dc", "v3", "dp1", true, c.getTime());
        c.set(2002, 05, 15, 15, 15);
        Representation r4 = insertRepresentation("c1", "dc", "v4", "dp1", true, c.getTime());
        List<Representation> foundRepresentations = solrDAO.search(
            RepresentationSearchParams.builder().setFromDate(r2.getCreationDate()).setToDate(r3.getCreationDate())
                    .build(), 0, 10);
        TestUtil.assertSameContent(foundRepresentations, Arrays.asList(r2, r3));
    }


    @Test
    public void shouldIterateThroughAllRepresentations()
            throws Exception {
        int count = 50;
        Set<Representation> generatedRepresentations = new HashSet<>(count, 1f);
        for (int i = 0; i < count; i++) {
            generatedRepresentations.add(insertRepresentation("id", "dc", UUID.randomUUID().toString(), "dp", true,
                new Date()));
        }
        RepresentationSearchParams searchParams = RepresentationSearchParams.builder().setSchema("dc").build();
        Set<Representation> foundRepresentations = new HashSet<>(count, 1f);
        boolean hasNext = true;
        int offset = 0;
        int pageLimit = 8;
        while (hasNext) {
            List<Representation> searchResults = solrDAO.search(searchParams, offset, pageLimit);
            foundRepresentations.addAll(searchResults);
            hasNext = !searchResults.isEmpty();
            offset += pageLimit;
        }
        assertThat(foundRepresentations.size(), is(count));
        assertThat(foundRepresentations, is(generatedRepresentations));
    }


    private Representation insertRepresentation(String cloudId, String schema, String version, String dataProvider,
            boolean persistent, Date date)
            throws IOException, SolrServerException {
        Representation rep = new Representation(cloudId, schema, version, null, null, dataProvider,
                new ArrayList<File>(), persistent, date);
        solrDAO.insertRepresentation(rep, null);
        return rep;
    }

}
