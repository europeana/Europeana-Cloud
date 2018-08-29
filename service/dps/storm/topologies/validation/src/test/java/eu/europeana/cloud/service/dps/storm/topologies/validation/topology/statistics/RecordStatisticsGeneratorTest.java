package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 1/10/2018.
 */
public class RecordStatisticsGeneratorTest {
    private static final int MAX_SIZE = 30000;
    private static Multimap<String, String> xpathValueMap;
    private static Multimap<String, Long> xpathOccurrenceMap;
    private static Multimap<String, Set<AttributeStatistics>> xpathAttributeMap;

    @BeforeClass
    public static void init() {
        initXpathValueMap();
        initXpathOccurrenceMap();
        initXpathAttributeMap();
    }

    @Test
    public void testGeneratedStatistics() throws Exception {
        String fileContent = readFile("src/test/resources/example1.xml");
        RecordStatisticsGenerator xmlParser = new RecordStatisticsGenerator(fileContent);
        List<NodeStatistics> nodeModelList = xmlParser.getStatistics();
        assertEquals(nodeModelList.size(), xpathValueMap.size());
        for (NodeStatistics nodeModel : nodeModelList) {

            assertNotNull(xpathValueMap.get(nodeModel.getXpath()));
            assertTrue(xpathValueMap.get(nodeModel.getXpath()).contains(nodeModel.getValue()));

            assertNotNull(xpathOccurrenceMap.get(nodeModel.getXpath()));
            assertTrue(nodeModel.getOccurrence() > 0);
            assertTrue(xpathOccurrenceMap.get(nodeModel.getXpath()).contains(nodeModel.getOccurrence()));

            assertEquals(getParentXpathFromXpath(nodeModel.getXpath()), nodeModel.getParentXpath());

            Set<AttributeStatistics> attributeModels = nodeModel.getAttributesStatistics();
            assertTrue(xpathAttributeMap.get(nodeModel.getXpath()).contains(attributeModels));

        }
    }


    @Test
    public void nodeContentsSizeShouldBeSmallerThanMaximumSize() throws Exception {
        String fileContent = readFile("src/test/resources/bigContent.xml");
        RecordStatisticsGenerator xmlParser = new RecordStatisticsGenerator(fileContent);
        List<NodeStatistics> nodeModelList = xmlParser.getStatistics();
        for (NodeStatistics nodeModel : nodeModelList) {
            assertTrue(nodeModel.getValue().length() <= MAX_SIZE);
        }

    }

    @Test
    public void checkIfTheNodesXpathsExist() throws Exception {
        String fileContent = readFile("src/test/resources/example1.xml");
        RecordStatisticsGenerator xmlParser = new RecordStatisticsGenerator(fileContent);
        List<NodeStatistics> nodeModelList = xmlParser.getStatistics();
        Document doc = createDocumentForSAX(fileContent);
        for (NodeStatistics nodeModel : nodeModelList) {
            XPath xPath = XPathFactory.newInstance().newXPath();
            String expression = nodeModel.getXpath();
            NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODE);
            assertNotNull(nodeList);
            assertTrue(nodeList.getLength() > 0);
        }
    }

    private Document createDocumentForSAX(String fileContent) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(new InputSource(new StringReader(fileContent)));
        doc.getDocumentElement().normalize();
        return doc;
    }

    private String readFile(String filePath) throws IOException {
        return IOUtils.toString(new FileInputStream(filePath));

    }

    private String getParentXpathFromXpath(String xpath) {
        if (StringUtils.countMatches(xpath, '/') < 3)
            return "";
        return xpath.substring(0, xpath.lastIndexOf('/'));
    }

    private static void initXpathAttributeMap() {
        xpathAttributeMap = ArrayListMultimap.create();
        Set<AttributeStatistics> set = new HashSet<>();
        xpathAttributeMap.put("//root/father1", set);
        xpathAttributeMap.put("//root/father3", set);
        xpathAttributeMap.put("//root", set);
        xpathAttributeMap.put("//root/father1", set);

        Set<AttributeStatistics> set1 = new HashSet<>();
        AttributeStatistics attributeModel1 = new AttributeStatistics("//root/father3/childA/@livesIn", "Poland", 1);
        AttributeStatistics attributeModel2 = new AttributeStatistics("//root/father3/childA/@livesIn", "Syria", 1);
        set1.add(attributeModel1);
        set1.add(attributeModel2);
        xpathAttributeMap.put("//root/father3/childA", set1);

        Set<AttributeStatistics> set2 = new HashSet<>();
        AttributeStatistics attributeModel = new AttributeStatistics("//root/father1/childA/@livesIn", "Poland", 1);
        set2.add(attributeModel);
        xpathAttributeMap.put("//root/father1/childA", set2);

        Set<AttributeStatistics> set3 = new HashSet<>();
        AttributeStatistics attributeModel3 = new AttributeStatistics("//root/father1/childA/@livesIn", "Netherlands", 1);
        set3.add(attributeModel3);
        xpathAttributeMap.put("//root/father1/childA", set3);
    }

    private static void initXpathOccurrenceMap() {
        xpathOccurrenceMap = ArrayListMultimap.create();
        xpathOccurrenceMap.put("//root/father1", 1l);
        xpathOccurrenceMap.put("//root/father3/childA", 2l);
        xpathOccurrenceMap.put("//root/father1/childA", 1l);
        xpathOccurrenceMap.put("//root/father3", 2l);
        xpathOccurrenceMap.put("//root", 1l);
        xpathOccurrenceMap.put("//root/father1/childA", 1l);
        xpathOccurrenceMap.put("//root/father1", 1l);
    }

    private static void initXpathValueMap() {
        xpathValueMap = ArrayListMultimap.create();
        xpathValueMap.put("//root/father1", "Val");
        xpathValueMap.put("//root/father3/childA", "Tarek");
        xpathValueMap.put("//root/father1/childA", "Valentine");
        xpathValueMap.put("//root/father3", "");
        xpathValueMap.put("//root", "");
        xpathValueMap.put("//root/father1/childA", "Mirjam");
        xpathValueMap.put("//root/father1", "Mir");
    }


}