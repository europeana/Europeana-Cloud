package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.MyOAIHarvester;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.XmlXPath;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import org.apache.commons.io.IOUtils;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.client.OAIClient;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class GzipTest {

    private static final String METADATA_XPATH = "/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']" +
            "/*[local-name()='metadata']" +
            "/child::*";
    private XPathExpression expr;

    @Before
    public void init() throws XPathExpressionException {
        XPath xpath = XPathFactory.newInstance().newXPath();
        expr = xpath.compile(METADATA_XPATH);
    }

    @Test
    public void shouldDownloadGzipped(){

        OAIClient client = new MyOAIHarvester("http://oai-pmh-test.eanadev.org/oai");
        GetRecordParameters params = new GetRecordParameters().withIdentifier("http://data.europeana.eu/item/0940441/_nnh0ZRW").withMetadataFormatPrefix("edm");

        try (final InputStream record = client.execute(Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params))) {
//            String theString = IOUtils.toString(record, "UTF-8");
//            System.out.println(theString);
//            InputStream inputStream = ByteSource.wrap(IOUtils.toByteArray(record)).openStream();
            InputStream xpath = new XmlXPath(record).xpath(expr);
            System.out.println(IOUtils.toString(xpath));
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
}
