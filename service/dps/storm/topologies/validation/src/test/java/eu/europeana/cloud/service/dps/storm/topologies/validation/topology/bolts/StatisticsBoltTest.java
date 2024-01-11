package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL_CLOUD_ID2;
import static org.mockito.Mockito.mock;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class StatisticsBoltTest extends CassandraTestBase {

  private static final long TASK_ID = 1;

  private static final String TASK_NAME = "task1";

  private ValidationStatisticsServiceImpl statisticsService;

  @Mock(name = "outputCollector")
  private OutputCollector collector;

  @InjectMocks
  private StatisticsBolt statisticsBolt = new StatisticsBolt(new CassandraProperties(),
      HOST, CassandraTestInstance.getPort(), KEYSPACE, "", "");

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    statisticsBolt.prepare();
    statisticsService = Mockito.spy(ValidationStatisticsServiceImpl.getInstance(
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, "",
            "")));
  }

  @Test
  public void testCountStatisticsSuccessfully() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, prepareStormTaskTupleParameters(),
        new Revision());
    List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

    //when
    statisticsBolt.execute(anchorTuple, tuple);

    //then
    assertSuccess(1);
    assertDataStoring(generated);
  }

  @Test
  public void testAggregatedCountStatisticsSuccessfully() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    Tuple anchorTuple2 = mock(TupleImpl.class);
    byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, prepareStormTaskTupleParameters(),
        new Revision());
    List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

    byte[] fileData2 = Files.readAllBytes(Paths.get("src/test/resources/example2.xml"));
    StormTaskTuple tuple2 = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL_CLOUD_ID2, fileData2,
        prepareStormTaskTupleParameters(), new Revision());
    List<NodeStatistics> generated2 = new RecordStatisticsGenerator(new String(fileData2)).getStatistics();

    //when
    statisticsBolt.execute(anchorTuple, tuple);
    statisticsBolt.execute(anchorTuple2, tuple2);

    //then
    assertSuccess(2);
    assertDataStoring(generated, generated2);
  }

  @Test
  public void shouldNotGenerateStatisticsBecauseOfLackOfTheParameter() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData,
        prepareStormTaskTupleParametersWithoutStatsParam(), new Revision());

    //when
    statisticsBolt.execute(anchorTuple, tuple);

    //then
    assertSuccess(1);
    Mockito.verifyNoInteractions(statisticsService);
  }

  @Test
  public void shouldNotGenerateStatisticsBecauseOfWrongParameter() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData,
        prepareStormTaskTupleParametersWithWrongStatsParam(), new Revision());

    //when
    statisticsBolt.execute(anchorTuple, tuple);

    //then
    Mockito.verifyNoInteractions(statisticsService);
  }

  @Test
  public void testCountStatisticsFailed() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    fileData[0] = 'X'; // will cause SAXException
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, prepareStormTaskTupleParameters(),
        new Revision());
    //when
    statisticsBolt.execute(anchorTuple, tuple);
    //then
    assertFailure();
  }

  private void assertDataStoring(List<NodeStatistics> generated, List<NodeStatistics> generated2) {
    List<NodeStatistics> statistics = statisticsService.getNodeStatistics(TASK_ID);

    Assert.assertEquals(statistics.size(), generated.size());
    for (NodeStatistics stats : statistics) {
      NodeStatistics nodeGenerated = findNode(stats, generated);
      long nodeGeneratedOccurrence = nodeGenerated != null ? nodeGenerated.getOccurrence() : 0;
      NodeStatistics nodeGenerated2 = findNode(stats, generated2);
      long nodeGeneratedOccurrence2 = nodeGenerated2 != null ? nodeGenerated2.getOccurrence() : 0;
      Assert.assertEquals(stats.getOccurrence(), nodeGeneratedOccurrence + nodeGeneratedOccurrence2);
      for (AttributeStatistics attrStats : stats.getAttributesStatistics()) {
        AttributeStatistics nodeAttrStats = findNodeAttributes(attrStats, nodeGenerated.getAttributesStatistics());
        long nodeAttrsOccurrence = nodeAttrStats != null ? nodeAttrStats.getOccurrence() : 0;
        AttributeStatistics nodeAttrStats2 = findNodeAttributes(attrStats, nodeGenerated2.getAttributesStatistics());
        long nodeAttrsOccurrence2 = nodeAttrStats2 != null ? nodeAttrStats2.getOccurrence() : 0;
        Assert.assertEquals(attrStats.getOccurrence(), nodeAttrsOccurrence + nodeAttrsOccurrence2);
      }
    }
  }

  private AttributeStatistics findNodeAttributes(AttributeStatistics attr, Set<AttributeStatistics> statistics) {
    for (AttributeStatistics attributeStatistics : statistics) {
      if (attributeStatistics.getName().equals(attr.getName()) &&
          attributeStatistics.getValue().equals(attr.getValue())) {
        return attributeStatistics;
      }
    }
    return null;
  }

  private NodeStatistics findNode(NodeStatistics node, List<NodeStatistics> statistics) {
    for (NodeStatistics nodeStatistics : statistics) {
      if (nodeStatistics.getParentXpath().equals(node.getParentXpath()) &&
          nodeStatistics.getXpath().equals(node.getXpath()) &&
          nodeStatistics.getValue().equals(node.getValue())) {
        return nodeStatistics;
      }
    }
    return null;
  }

  private void assertDataStoring(List<NodeStatistics> generated) {
    List<NodeStatistics> statistics = statisticsService.getNodeStatistics(TASK_ID);
    Assert.assertEquals(statistics.size(), generated.size());
    Assert.assertTrue(statistics.containsAll(generated));
  }

  private void assertSuccess(int times) {
    Mockito.verify(collector, Mockito.times(times)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
    Mockito.verify(collector, Mockito.times(0))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
  }

  private void assertFailure() {
    Mockito.verify(collector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
    Mockito.verify(collector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    parameters.put(PluginParameterKeys.GENERATE_STATS, "true");
    return parameters;
  }

  private HashMap<String, String> prepareStormTaskTupleParametersWithoutStatsParam() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }

  private HashMap<String, String> prepareStormTaskTupleParametersWithWrongStatsParam() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    parameters.put(PluginParameterKeys.GENERATE_STATS, "trues");
    return parameters;
  }
}
