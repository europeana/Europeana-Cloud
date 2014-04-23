/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.europeana.cloud.service.dls.aspects;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dls.solr.RepresentationSolrDocument;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
import eu.europeana.cloud.service.dls.solr.exception.SystemException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import junitparams.JUnitParamsRunner;
import static junitparams.JUnitParamsRunner.$;
import junitparams.Parameters;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@RunWith(JUnitParamsRunner.class)
public class ServiceExceptionTranslatorTest {

    private SolrDAO solrDAO;
    private SolrServer solrServer;
    private QueryResponse response;


    @Before
    public void setUp()
            throws Exception {
        final ApplicationContext applicationContext = new ClassPathXmlApplicationContext(
                "classpath:/testAspectsContext.xml");
        solrDAO = applicationContext.getBean(SolrDAO.class);
        solrServer = applicationContext.getBean(SolrServer.class);
        response = mock(QueryResponse.class);
        final List<RepresentationSolrDocument> resultList = new ArrayList();
        resultList.add(new RepresentationSolrDocument());
        doReturn(resultList).when(response).getBeans(any(Class.class));
    }


    private Object[] exceptionList2() {
        return $($(new SolrException(ErrorCode.SERVER_ERROR, "test")), $(new SolrServerException("test"))
        //
        );
    }


    private Object[] exceptionList3() {
        return $($(new SolrException(ErrorCode.SERVER_ERROR, "test")), $(new SolrServerException("test")),
            $(new IOException())
        //
        );
    }


    @Test
    @Parameters(method = "exceptionList3")
    public void shouldTranslateExceptionInInsertRepresentation(final Exception testingException)
            throws Exception {
        //prepare failure
        if (testingException instanceof IOException) {
            doReturn(mock(QueryResponse.class)).when(solrServer).query(any(SolrParams.class));
            doThrow(testingException).when(solrServer).commit();
        } else
            doThrow(testingException).when(solrServer).query(any(SolrParams.class));

        // execute method to throw prepared exception and catch it
        try {
            solrDAO.insertRepresentation(mock(Representation.class), new HashSet<CompoundDataSetId>());
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList2")
    public void shouldTranslateExceptionInGetDocumentById(final Exception testingException)
            throws Exception {
        //prepare failure
        doThrow(testingException).when(solrServer).query(any(SolrParams.class));
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.getDocumentById("versionId");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList2")
    public void shouldTranslateExceptionInAddAssignment(final Exception testingException)
            throws Exception {
        //prepare failure
        doThrow(testingException).when(solrServer).query(any(SolrParams.class));
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.addAssignment("versionId", new CompoundDataSetId("dataSetProviderId", "dataSetId"));
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList2")
    public void shouldTranslateExceptionInRemoveRepresentationVersion(final Exception testingException)
            throws Exception {
        //prepare failure
        doThrow(testingException).when(solrServer).deleteById(anyString());
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.removeRepresentationVersion("versionId");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList2")
    public void shouldTranslateExceptionInRemoveRepresentation(final Exception testingException)
            throws Exception {
        //prepare failure
        doThrow(testingException).when(solrServer).deleteByQuery(anyString());
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.removeRepresentation("cloudId", "schema");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList2")
    public void shouldTranslateExceptionInRemoveRecordRepresentation(final Exception testingException)
            throws Exception {
        //prepare failure
        doThrow(testingException).when(solrServer).deleteByQuery(anyString());
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.removeRecordRepresentation("cloudId");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList3")
    public void shouldTranslateExceptionInRemoveAssignmentSC(final Exception testingException)
            throws Exception {
        //prepare failure
        if (testingException instanceof IOException) {
            doReturn(response).when(solrServer).query(any(SolrParams.class));
            doThrow(testingException).when(solrServer).commit();
        } else {
            doThrow(testingException).when(solrServer).query(any(SolrParams.class));
        }
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.removeAssignment("versionId", new CompoundDataSetId("dataSetProviderId", "dataSetId"));
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList3")
    public void shouldTranslateExceptionInRemoveAssignmentSSC(final Exception testingException)
            throws Exception {
        //prepare failure
        if (testingException instanceof IOException) {
            doReturn(response).when(solrServer).query(any(SolrParams.class));
            doThrow(testingException).when(solrServer).commit();
        } else {
            doThrow(testingException).when(solrServer).query(any(SolrParams.class));
        }
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.removeAssignment("versionId", "schema", new HashSet<CompoundDataSetId>());
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test
    @Parameters(method = "exceptionList3")
    public void shouldTranslateExceptionInRemoveAssignmentFromDataSet(final Exception testingException)
            throws Exception {
        //prepare failure
        if (testingException instanceof IOException) {
            doReturn(response).when(solrServer).query(any(SolrParams.class));
            doThrow(testingException).when(solrServer).commit();
        } else {
            doThrow(testingException).when(solrServer).query(any(SolrParams.class));
        }
        // execute method to throw prepared exception and catch it
        try {
            solrDAO.removeAssignmentFromDataSet(new CompoundDataSetId("dataSetProviderId", "dataSetId"));
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertEquals(testingException, e.getCause());
        }
    }


    @Test(expected = SolrDocumentNotFoundException.class)
    public void shouldNotTranslateExceptionInGetDocumentById()
            throws Exception {
        //prepare failure
        doReturn(response).when(solrServer).query(any(SolrParams.class));
        final List<RepresentationSolrDocument> resultEmptyList = new ArrayList();
        doReturn(resultEmptyList).when(response).getBeans(any(Class.class));
        // execute method to throw prepared exception
        solrDAO.getDocumentById("versionId");
    }


    @Test(expected = SolrDocumentNotFoundException.class)
    public void shouldNotTranslateExceptionInRemoveAssignmentSC()
            throws Exception {
        //prepare failure
        doReturn(response).when(solrServer).query(any(SolrParams.class));
        final List<RepresentationSolrDocument> resultEmptyList = new ArrayList();
        doReturn(resultEmptyList).when(response).getBeans(any(Class.class));
        // execute method to throw prepared exception
        solrDAO.removeAssignment("versionId", new CompoundDataSetId("dataSetProviderId", "dataSetId"));
    }


    @Test(expected = SolrDocumentNotFoundException.class)
    public void shouldNotTranslateExceptionInAddAssignment()
            throws Exception {
        //prepare failure
        doReturn(response).when(solrServer).query(any(SolrParams.class));
        final List<RepresentationSolrDocument> resultEmptyList = new ArrayList();
        doReturn(resultEmptyList).when(response).getBeans(any(Class.class));
        // execute method to throw prepared exception
        solrDAO.addAssignment("versionId", new CompoundDataSetId("dataSetProviderId", "dataSetId"));
    }
}
