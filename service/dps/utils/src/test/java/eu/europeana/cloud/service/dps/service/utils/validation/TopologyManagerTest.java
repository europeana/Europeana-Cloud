package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


public class TopologyManagerTest {

  final String nameList = "topologyA,topologyB";
  TopologyManager instance;

  @BeforeEach
  void init() {
    instance = new TopologyManager(nameList);
  }

  @Test
  void shouldCreateInstanceWithSuccess() {
    //given
    //when
    //then
    assertThat(instance, is(notNullValue()));
  }


  @Test
  void shouldRetrieveTopologyNamesWithSuccess() {
    //given
    //when
    List<String> resultNameList = instance.getNames();
    //then
    List<String> expectedNameList = convertStringToList(nameList);
    assertThat(resultNameList, is(equalTo(expectedNameList)));
  }


  @Test
  void shouldContainTopologyA() {
    baseContainsTopologyTest("topologyA", true);
  }

  @Test
  void shouldContainTopologyB() {
    baseContainsTopologyTest("topologyB", true);
  }

  @Test
  void shouldNotContainTopologyC() {
    baseContainsTopologyTest("topologyC", false);
  }

  private void baseContainsTopologyTest(String topologyName, boolean expectedValue) {
    //given
    //      topologyName
    //when
    boolean result = instance.containsTopology(topologyName);
    //then
    assertThat(result, is(equalTo(expectedValue)));

  }

  private List<String> convertStringToList(String input) {
    return Arrays.asList(getSplit(input));
  }

  private String[] getSplit(String input) {
    return input.split(TopologyManager.separatorChar);
  }
}