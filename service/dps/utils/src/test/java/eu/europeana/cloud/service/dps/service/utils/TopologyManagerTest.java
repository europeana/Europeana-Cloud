package eu.europeana.cloud.service.dps.service.utils;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


public class TopologyManagerTest {


    final String nameList = "topologyA,topologyB";
    TopologyManager instance;

    @Before
    public void init() {
        instance = new TopologyManager(nameList);
    }

    @Test
    public void should_ProperlyCreateInstance() {
        //given
        //when
        //then
        assertThat(instance, is(notNullValue()));
    }


    @Test
    public void should_successfully_getTopologyNames() {
        //given
        //when
        List<String> resultNameList = instance.getNames();
        //then
        List<String> expectedNameList = convertStringToList(nameList);
        assertThat(resultNameList, is(equalTo(expectedNameList)));
    }


    @Test
    public void should_successfully_containsTopology1() {
        //given
        final String topologyName = "topologyA";
        //when
        boolean result = instance.containsTopology(topologyName);
        //then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void should_successfully_containsTopology2() {
        //given
        final String topologyName = "topologyB";
        //when
        boolean result = instance.containsTopology(topologyName);
        //then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void should_unsuccessfully_containsTopology() {
        //given
        final String topologyName = "topologyC";
        //when
        boolean result = instance.containsTopology(topologyName);
        //then
        assertThat(result, is(equalTo(false)));
    }

    private List<String> convertStringToList(String input) {
        return Arrays.asList(getSplit(input));
    }

    private String[] getSplit(String input) {
        return input.split(TopologyManager.separatorChar);
    }


}