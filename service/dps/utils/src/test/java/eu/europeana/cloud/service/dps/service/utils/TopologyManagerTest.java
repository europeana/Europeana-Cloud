package eu.europeana.cloud.service.dps.service.utils;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


public class TopologyManagerTest {

    final String userList = "userA,userB";
    final String nameList = "topologyA,topologyB";
    TopologyManager instance;

    @Before
    public void init() {
        instance = new TopologyManager(nameList, userList);
    }

    @Test
    public void should_ProperlyCreateInstance() {
        //given
        //when
        //then
        assertThat(instance, is(notNullValue()));
    }

    @Test
    public void should_throwAssertionError_OnInstanceCreation() {
        //given
        final String brokenUserList = "userA";
        //when
        try {
            final TopologyManager result = new TopologyManager(nameList, brokenUserList);
        } catch (AssertionError exception) {
            assertThat(exception, is(instanceOf(AssertionError.class)));
        }
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
    public void should_successfully_getUserNames() {
        //given
        //when
        List<String> resultUserList = instance.getUserNames();
        //then
        List<String> expectedUserList = convertStringToList(userList);
        assertThat(resultUserList, is(equalTo(expectedUserList)));
    }

    @Test
    public void should_successfully_getNameToUserMapping() {
        //given
        //when
        Map<String, String> resultNameToUserMap = instance.getNameToUserMap();
        //then
        Map<String, String> expectedNameToUserMap = constructMap();
        assertThat(resultNameToUserMap, is(equalTo(expectedNameToUserMap)));
    }

    private Map<String, String> constructMap() {
        Map<String, String> expectedNameToUserMap;
        expectedNameToUserMap = new HashMap<>();
        String[] names = getSplit(nameList);
        String[] userNames = getSplit(userList);
        for (int i = 0; i < names.length; i++) {
            expectedNameToUserMap.put(names[i], userNames[i]);
        }
        return expectedNameToUserMap;
    }

    private List<String> convertStringToList(String input) {
        return Arrays.asList(getSplit(input));
    }

    private String[] getSplit(String input) {
        return input.split(TopologyManager.separatorChar);
    }


}