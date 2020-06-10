package eu.europeana.cloud.service.dps.oaipmh;

import net.sf.saxon.om.NodeInfo;
import org.xml.sax.InputSource;

import javax.xml.transform.*;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.*;
import java.util.ArrayList;

/**
 * Class xpath on XML passed as {@code InputStream}.
 *
 * @author krystian.
 */
class XmlXPath {

    private final String input;

    XmlXPath(String input) {
        this.input = input;
    }

    /**
     * Xpath on xml based on passed expression.
     *
     * @param expr expression
     * @return result of xpath
     * @throws HarvesterException in case there is a problem with the expression.
     */
    InputStream xpathToStream(XPathExpression expr) throws HarvesterException {
        try {
            final InputSource inputSource = getInputSource();
            final ArrayList<NodeInfo> result = (ArrayList<NodeInfo>) expr.evaluate(inputSource, XPathConstants.NODESET);
            return convertToStream(result);
        } catch (XPathExpressionException | TransformerException e) {
            throw new HarvesterException("Cannot xpath XML!", e);
        }
    }

    /**
     * Xpath on xml based on passed expression.
     *
     * @param expr expression
     * @return result of xpath
     * @throws HarvesterException in case there is a problem with the expression.
     */
    String xpathToString(XPathExpression expr) throws HarvesterException {
        try {
            return evaluateExpression(expr);
        } catch (XPathExpressionException e) {
            throw new HarvesterException("Cannot xpath XML!", e);
        }
    }

    private InputSource getInputSource() {
        return new SAXSource(new InputSource(new StringReader(input))).getInputSource();
    }

    private String evaluateExpression(XPathExpression expr) throws XPathExpressionException {
        final InputSource inputSource = getInputSource();
        return expr.evaluate(inputSource);
    }

    private InputStream convertToStream(ArrayList<NodeInfo> nodes) throws TransformerException, HarvesterException {
        final int length = nodes.size();
        if (length < 1) {
            throw new HarvesterException("Empty XML!");
        } else if (length > 1) {

            throw new HarvesterException("More than one XML!");
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Result outputTarget = new StreamResult(outputStream);
            TransformerFactory transformerFactory = new net.sf.saxon.TransformerFactoryImpl();

            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(nodes.get(0), outputTarget);

            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (IOException e) {
            // Cannot really happen.
            throw new HarvesterException("Unexpected exception", e);
        }
    }
}
