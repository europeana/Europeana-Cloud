package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class xpath on XML passed as {@code InputStream}.
 *
 * @author krystian.
 */
public class XmlXPath {
    private final InputStream input;

     public XmlXPath(InputStream input) {
        this.input = input;
    }

    /**
     * Xpath on xml based on passed expression.
     *
     * @param expr expression
     * @return result of xpath
     * @throws HarvesterException
     */
    public InputStream xpath(XPathExpression expr) throws HarvesterException, IOException {
        try {
            final InputSource inputSource = new SAXSource(new InputSource(input)).getInputSource();
            final NodeList result = (NodeList) expr.evaluate(inputSource,
                    XPathConstants.NODESET);

            return convertToStream(result);
        } catch (XPathExpressionException | TransformerException e) {
            throw new HarvesterException("Cannot xpath XML!", e);
        } finally {
            if (input != null) {
                input.close();
            }
        }
    }

    private InputStream convertToStream(NodeList nodes) throws TransformerException, HarvesterException, IOException {
        final int length = nodes.getLength();
        if (length < 1) {
            throw new HarvesterException("Empty XML!");
        } else if (length > 1) {
            throw new HarvesterException("More than one XML!");
        }
        try (
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();) {
            Source xmlSource = new DOMSource(nodes.item(0));
            Result outputTarget = new StreamResult(outputStream);
            TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
            return new ByteArrayInputStream(outputStream.toByteArray());
        }
    }
}
