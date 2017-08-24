package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.TestHelper.convertToString;
import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.TestHelper.isSimilarXml;
import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.WiremockHelper.getFileContent;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author krystian.
 */
public class XmlXPathTest {
    private static final String EXPRESSION = "/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']"+
            "/*[local-name()='metadata']" +
            "/child::*";
    private static final String ENCODING = "UTF-8";

    @Test
    public void shouldFilterOaiDcResponse() throws IOException, HarvesterException {
        //given
        final String fileContent = getFileContent("/sampleOaiRecord.xml");
        final InputStream inputStream = IOUtils.toInputStream(fileContent, ENCODING);

        //when
        final InputStream result = new XmlXPath(inputStream).xpath(EXPRESSION);

        //then
        final String actual = convertToString(result);
        assertThat(actual, isSimilarXml(getFileContent("/expectedOaiRecord.xml")));
    }

    @Test
    public void shouldThrowExceptionNonSingleOutputCandidate() throws IOException, HarvesterException {
        //given
        final String fileContent = getFileContent("/sampleOaiRecord.xml");
        final InputStream inputStream = IOUtils.toInputStream(fileContent, ENCODING);

        try {
            //when
            new XmlXPath(inputStream).xpath("/some/bad/xpath");
            fail();
        }catch (HarvesterException e){
            //then
            assertThat(e.getMessage(),is("Empty XML!"));
        }
    }

    @Test
    public void shouldThrowExceptionOnEmpty() throws IOException, HarvesterException {
        //given
        final String fileContent = "";
        final InputStream inputStream = IOUtils.toInputStream(fileContent, ENCODING);

        try {
            //when
            new XmlXPath(inputStream).xpath(EXPRESSION);
            fail();
        }catch (HarvesterException e){
            //then
            assertThat(e.getMessage(),is("Cannot xpath XML!"));
        }
    }


}