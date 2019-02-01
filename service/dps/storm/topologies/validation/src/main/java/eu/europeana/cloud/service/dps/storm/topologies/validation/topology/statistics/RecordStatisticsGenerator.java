package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics;

import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Created by Tarek on 1/9/2018.
 */
public class RecordStatisticsGenerator {
    private Map<String, NodeStatistics> nodeStatistics;
    private String fileContent;
    private static int MAX_SIZE = 1000;

    public RecordStatisticsGenerator(String fileContent) {
        this.fileContent = fileContent;
        nodeStatistics = new HashMap<>();

    }

    public List<NodeStatistics> getStatistics() throws SAXException, IOException, ParserConfigurationException {
        Document doc = getParsedDocument();
        doc.getDocumentElement().normalize();
        Node root = doc.getDocumentElement();
        addRootToNodeList(root);
        prepareNodeStatistics(root);
        return new ArrayList<>(nodeStatistics.values());
    }


    private Document getParsedDocument() throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(fileContent));
        return db.parse(is);
    }

    private void addRootToNodeList(Node root) {
        String nodeXpath = getXPath(root);
        String nodeValue = getFirstLevelTextContent(root);
        String modelKey = getKey(nodeXpath, nodeValue);
        nodeStatistics.put(modelKey, new NodeStatistics("", nodeXpath, nodeValue, 1));
    }

    private void prepareNodeStatistics(Node root) {
        String parentXpath = getXPath(root);
        NodeList childrenNodes = root.getChildNodes();
        for (int i = 0; i < childrenNodes.getLength(); i++) {
            Node node = childrenNodes.item(i);
            if (node instanceof Element) {
                String nodeXpath = getXPath(node);
                String nodeValue = getFirstLevelTextContent(node);
                String modelKey = getKey(nodeXpath, nodeValue);
                NodeStatistics nodeModel = nodeStatistics.get(modelKey);
                if (nodeModel == null) {
                    nodeModel = new NodeStatistics(parentXpath, nodeXpath, nodeValue, 1);
                } else {
                    nodeModel.increaseOccurrence();
                }
                assignAttributesToNode(nodeModel.getAttributesStatistics(), node.getAttributes());
                nodeStatistics.put(modelKey, nodeModel);
                prepareNodeStatistics(node);
            }
        }
    }


    private void assignAttributesToNode(Set<AttributeStatistics> existedAttributes, NamedNodeMap attributes) {
        for (int j = 0; j < attributes.getLength(); j++) {
            Attr attr = (Attr) attributes.item(j);
            String attrName = getAttributeXPath(attr);
            String attrValue = attr.getNodeValue();
            if (existedAttributes.isEmpty()) {
                AttributeStatistics attributeModel = new AttributeStatistics(attrName, attrValue, 1);
                existedAttributes.add(attributeModel);
            } else {
                AttributeStatistics attributeModel = new AttributeStatistics(attrName, attrValue, 1);
                if (existedAttributes.contains(attributeModel)) {
                    increaseOccurrence(existedAttributes, attributeModel);
                } else
                    existedAttributes.add(attributeModel);
            }
        }
    }

    private String getXPath(Node node) {
        Node parent = node.getParentNode();
        if (parent == null) {
            return "/";
        }
        return getXPath(parent) + "/" + node.getNodeName();
    }

    private String getKey(String name, String value) {
        String key = name + "-" + value;
        return String.valueOf(key.hashCode());
    }

    private String getAttributeXPath(Attr attr) {
        Node owner = attr.getOwnerElement();
        return getXPath(owner) + "/@" + attr.getNodeName();
    }


    private void increaseOccurrence(Set<AttributeStatistics> models, AttributeStatistics comparableAttributeModel) {
        for (AttributeStatistics attributeModel : models) {
            if (attributeModel.equals(comparableAttributeModel)) {
                attributeModel.increaseOccurrence();
                break;
            }
        }
    }

    private String getFirstLevelTextContent(Node node) {
        NodeList list = node.getChildNodes();
        StringBuilder textContent = new StringBuilder();
        for (int i = 0; i < list.getLength(); ++i) {
            Node child = list.item(i);
            if (child.getNodeType() == Node.TEXT_NODE) {
                textContent.append(child.getTextContent());
            }
        }

        if (textContent.length() > MAX_SIZE) {
            return textContent.substring(0, MAX_SIZE).trim();
        }
        return textContent.toString().trim();
    }
}
