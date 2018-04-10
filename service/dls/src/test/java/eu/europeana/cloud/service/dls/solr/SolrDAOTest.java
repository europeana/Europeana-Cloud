package eu.europeana.cloud.service.dls.solr;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dls.TestUtil;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/solrTestContext.xml" })
public class SolrDAOTest {

    @Autowired
    private SolrDAO solrDAO;

    @Autowired
    private SolrConnectionProvider connectionProvider;


    @Test
    public void shouldInsertAndReturnRepresentation()
            throws Exception {
        Representation rep = new Representation("cloud1", "schema1", "version1", null, null, "dataProvider", null,null,
                true, new Date());
        ArrayList<CompoundDataSetId> dataSets = new ArrayList<>();
        dataSets.add(new CompoundDataSetId("provider", "dataSet1"));
        dataSets.add(new CompoundDataSetId("provider", "dataSet2"));
        solrDAO.insertRepresentation(rep, dataSets);
        RepresentationSolrDocument doc = solrDAO.getDocumentById(rep.getVersion());
        assertEquals(rep.getVersion(), doc.getVersion());
        assertEquals(rep.getRepresentationName(), doc.getSchema());
        assertEquals(rep.getDataProvider(), doc.getProviderId());
        assertEquals(rep.getCreationDate(), doc.getCreationDate());
    }


    @Test(expected = SolrException.class)
    public void shouldThrowExceptionWhenRequiredFieldMissing()
            throws Exception {
        Representation rep = new Representation(null, "schema", "version", null, null, "dataProvider", null,null, true,
                new Date());
        solrDAO.insertRepresentation(rep, null);
    }


    @Test
    public void shouldAddAssignment()
            throws Exception {
        Representation rep = new Representation("cloud2", "schema1", "version2", null, null, "dataProvider", null,null,
                true, new Date());
        ArrayList<CompoundDataSetId> dataSets = new ArrayList<>();
        dataSets.add(new CompoundDataSetId("provider", "dataSet1"));
        dataSets.add(new CompoundDataSetId("provider", "dataSet2"));

        //insert representation with 2 datasets
        solrDAO.insertRepresentation(rep, dataSets);
        RepresentationSolrDocument doc = solrDAO.getDocumentById(rep.getVersion());
        
        List<String> docsDatasets = new ArrayList<>(doc.getDataSets());
        Collections.sort(docsDatasets);

        TestUtil.assertSameContent(docsDatasets, Lists.transform(dataSets, serialize));

        //add assigment to representation
        solrDAO.addAssignment(rep.getVersion(), new CompoundDataSetId("provider", "dataSet3"));
        RepresentationSolrDocument updatedDoc = solrDAO.getDocumentById(rep.getVersion());
        dataSets.add(new CompoundDataSetId("provider", "dataSet3"));
        
        List<String> updatedDocDatasets = new ArrayList<>(updatedDoc.getDataSets());
        Collections.sort(updatedDocDatasets);
        
        TestUtil.assertSameContent(updatedDocDatasets, Lists.transform(dataSets, serialize));
    }


    @Test
    public void shouldMakeOneSolrEntryIfAssignedMoreThanOnce()
            throws Exception {

        Representation rep = new Representation("cloud333", "schema1", "version2", null, null, "dataProvider", null,null,
                true, new Date());
        ArrayList<CompoundDataSetId> dataSets = new ArrayList<>();
        dataSets.add(new CompoundDataSetId("provider", "dataSet1"));
        dataSets.add(new CompoundDataSetId("provider", "dataSet2"));

        //given - representation with 2 datasets already assigned
        solrDAO.insertRepresentation(rep, dataSets);
        RepresentationSolrDocument doc = solrDAO.getDocumentById(rep.getVersion());
        
        List<String> docsDatasets = new ArrayList<>(doc.getDataSets());
        Collections.sort(docsDatasets);        
        
        TestUtil.assertSameContent(docsDatasets, Lists.transform(dataSets, serialize));

        //when (add the same assigment for the second time)
        solrDAO.addAssignment(rep.getVersion(), new CompoundDataSetId("provider", "dataSet1"));

        //then
        RepresentationSolrDocument updatedDoc = solrDAO.getDocumentById(rep.getVersion());
        
        List<String> updatedDocDatasets = new ArrayList<>(updatedDoc.getDataSets());
        Collections.sort(updatedDocDatasets);    
        
        TestUtil.assertSameContent(updatedDocDatasets, Lists.transform(dataSets, serialize));

    }


    @Test
    public void shouldRemoveRepresentation()
            throws Exception {
        Representation rep = new Representation("cloud1", "schema1", "version4", null, null, "dataProvider", null,null,
                true, new Date());
        solrDAO.insertRepresentation(rep, null);
        //check if doc got inserted
        RepresentationSolrDocument doc = solrDAO.getDocumentById(rep.getVersion());
        assertEquals(rep.getVersion(), doc.getVersion());

        solrDAO.removeRepresentationVersion(rep.getVersion());

        //try to access removed doc
        SolrDocumentNotFoundException ex = null;
        try {
            doc = solrDAO.getDocumentById(rep.getVersion());
        } catch (SolrDocumentNotFoundException e) {
            ex = e;
        }
        assertNotNull(ex);
    }


    @Test
    public void shouldRemoveAssignment()
            throws Exception {
        CompoundDataSetId dataSet1 = new CompoundDataSetId("provider", "dataSet1");
        CompoundDataSetId dataSet2 = new CompoundDataSetId("provider", "dataSet2");
        CompoundDataSetId dataSet3 = new CompoundDataSetId("provider", "dataSet3");

        Representation rep = new Representation("cloud2", "schema1", "version123", null, null, "dataProvider", null,null,
                true, new Date());
        ArrayList<CompoundDataSetId> dataSets = new ArrayList<>();

        dataSets.add(dataSet1);
        dataSets.add(dataSet2);
        dataSets.add(dataSet3);

        //insert representation with 2 datasets
        solrDAO.insertRepresentation(rep, dataSets);
        RepresentationSolrDocument doc = solrDAO.getDocumentById(rep.getVersion());
        
        List<String> docsDatasets = new ArrayList<>(doc.getDataSets());
        Collections.sort(docsDatasets);

        TestUtil.assertSameContent(docsDatasets, Lists.transform(dataSets, serialize));

        //remove assigment to representation
        solrDAO.removeAssignment(rep.getVersion(), dataSet2);
        RepresentationSolrDocument updatedDoc = solrDAO.getDocumentById(rep.getVersion());
        TestUtil.assertSameContent(updatedDoc.getDataSets(),
            Lists.transform(Arrays.asList(dataSet1, dataSet3), serialize));
    }


    @Test
    public void shouldRemoveAllRepresentationVersions()
            throws Exception {
        //create versions
        String schema = "commonSchema";
        String cloudId = "commonCloudId";
        Representation rep1 = new Representation(cloudId, schema, "v1.1", null, null, "dataProvider", null,null, true,
                new Date());
        Representation rep2 = new Representation(cloudId, schema, "v1.2", null, null, "dataProvider", null,null, true,
                new Date());
        Representation rep3 = new Representation(cloudId, schema, "v1.3", null, null, "dataProvider", null,null, true,
                new Date());
        solrDAO.insertRepresentation(rep1, null);
        solrDAO.insertRepresentation(rep2, null);
        solrDAO.insertRepresentation(rep3, null);
        solrDAO.removeRepresentation(cloudId, schema);

        SolrDocumentNotFoundException ex = null;
        try {
            solrDAO.getDocumentById(rep1.getVersion());
        } catch (SolrDocumentNotFoundException e) {
            ex = e;
        }
        assertNotNull(ex);
        ex = null;
        try {
            solrDAO.getDocumentById(rep2.getVersion());
        } catch (SolrDocumentNotFoundException e) {
            ex = e;
        }
        assertNotNull(ex);
        try {
            solrDAO.getDocumentById(rep3.getVersion());
        } catch (SolrDocumentNotFoundException e) {
            ex = e;
        }
        assertNotNull(ex);
    }


    @Test
    public void shouldRewriteRepresentationsOnNewVersion()
            throws IOException, SolrServerException, SolrDocumentNotFoundException {
        CompoundDataSetId ds1 = new CompoundDataSetId("provider", "dataSet1");
        CompoundDataSetId ds2 = new CompoundDataSetId("provider", "dataSet2");
        CompoundDataSetId ds3 = new CompoundDataSetId("provider", "dataSet3");
        CompoundDataSetId ds4 = new CompoundDataSetId("provider", "dataSet4");

        // insert persistent representation with 2 data sets
        Representation rep = new Representation("1", "dc", "v1", null, null, "dataProvider", null,null, true, new Date());
        solrDAO.insertRepresentation(rep, Arrays.asList(ds1, ds2));

        // insert new temporary version of representation with 2 other data sets
        Representation repNew = new Representation("1", "dc", "v2", null, null, "dataProvider", null,null, false, new Date());
        solrDAO.insertRepresentation(repNew, Arrays.asList(ds3, ds4));

        // now, persist the most recent version. Rewrite dataset ds2 to the newer version
        repNew.setPersistent(true);
        solrDAO.removeAssignment(rep.getVersion(), ds2);
        solrDAO.insertRepresentation(repNew, Arrays.asList(ds2));

        // then: old representation should contain only ds1, new: ds2, ds3 and ds4
        RepresentationSolrDocument repDocument = solrDAO.getDocumentById(rep.getVersion());
        RepresentationSolrDocument repNewDocument = solrDAO.getDocumentById(repNew.getVersion());
        TestUtil.assertSameContent(repDocument.getDataSets(), Lists.transform(Arrays.asList(ds1), serialize));
        
        List<String> newDocDatasets = new ArrayList<>(repNewDocument.getDataSets());
        Collections.sort(newDocDatasets);
        
        TestUtil.assertSameContent(newDocDatasets,
            Lists.transform(Arrays.asList(ds2, ds3, ds4), serialize));
    }


    @After
    public void deleteData()
            throws IOException, SolrServerException {
        connectionProvider.getSolrServer().deleteByQuery("*:*");
        connectionProvider.getSolrServer().commit();
    }


    private final Function<String, CompoundDataSetId> deserialize = new Function<String, CompoundDataSetId>() {

        @Override
        public CompoundDataSetId apply(String input) {
            return solrDAO.deserialize(input);
        }
    };

    private final Function<CompoundDataSetId, String> serialize = new Function<CompoundDataSetId, String>() {

        @Override
        public String apply(CompoundDataSetId input) {
            return solrDAO.serialize(input);
        }

    };

}
