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
import java.io.InputStream;

/**
 * Class xpath on XML passed as {@code InputStream}.
 *
 * @author krystian.
 */
class XmlXPath {
    private final InputStream input;

    XmlXPath(InputStream input) {
        this.input = input;
    }

    /**
     * Xpath on xml based on passed expression.
     *
     * @param expression expression
     * @return result of xpath
     * @throws HarvesterException
     */
    InputStream xpath(String expression) throws HarvesterException {
        try {
            final XPathExpression expr = compileExpression(expression);

            final InputSource inputSource = new SAXSource(new InputSource(input)).getInputSource();
            final NodeList result = (NodeList) expr.evaluate(inputSource,
                    XPathConstants.NODESET);

            return produceOutput(result);
        } catch (XPathExpressionException | TransformerException e) {
            throw new HarvesterException("Cannot xpath XML!", e);
        }
    }

    private XPathExpression compileExpression(String expression) throws XPathExpressionException {
        XPath xpath = XPathFactory.newInstance().newXPath();
        return xpath.compile(expression);
    }

    private InputStream produceOutput(NodeList node) throws TransformerException, HarvesterException {
        final int length = node.getLength();
        if(length < 1){
            throw new HarvesterException("Empty XML!");
        } else if(length > 1){
            throw new HarvesterException("More than one XML!");
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Source xmlSource = new DOMSource(node.item(0));
        Result outputTarget = new StreamResult(outputStream);
        TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
        return new ByteArrayInputStream(outputStream.toByteArray());
    }
}
