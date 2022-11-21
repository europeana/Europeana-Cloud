package eu.europeana.cloud.common.selectors;

import eu.europeana.cloud.common.model.Representation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LatestPersistentRepresentationVersionSelectorTest {


  RepresentationSelector representationSelector = new LatestPersistentRepresentationVersionSelector();
  List<Representation> emptyRepresentationsList = Collections.emptyList();
  List<Representation> representationsListWithZeroPersistentVersions = new ArrayList<>(2);
  List<Representation> representationsListWithOnePersistentVersion = new ArrayList<>(3);
  List<Representation> representationsListWithMultiplePersistentVersions = new ArrayList<>(4);

  @Before
  public void prepare() {
    Representation persistentVersion_1 = new Representation();
    persistentVersion_1.setVersion(new com.eaio.uuid.UUID().toString());
    persistentVersion_1.setPersistent(true);
    persistentVersion_1.setRepresentationName("name1");
    //
    Representation persistentVersion_2 = new Representation();
    persistentVersion_2.setVersion(new com.eaio.uuid.UUID().toString());
    persistentVersion_2.setPersistent(true);
    persistentVersion_2.setRepresentationName("name2");
    //
    Representation non_persistentVersion_1 = new Representation();
    non_persistentVersion_1.setVersion(new com.eaio.uuid.UUID().toString());
    non_persistentVersion_1.setRepresentationName("name3");
    //
    Representation non_persistentVersion_2 = new Representation();
    non_persistentVersion_2.setVersion(new com.eaio.uuid.UUID().toString());
    non_persistentVersion_2.setRepresentationName("name4");
    //
    representationsListWithOnePersistentVersion.add(persistentVersion_1);
    representationsListWithOnePersistentVersion.add(non_persistentVersion_1);
    representationsListWithOnePersistentVersion.add(non_persistentVersion_2);
    //
    representationsListWithMultiplePersistentVersions.add(persistentVersion_1);
    representationsListWithMultiplePersistentVersions.add(persistentVersion_2);
    representationsListWithMultiplePersistentVersions.add(non_persistentVersion_1);
    representationsListWithMultiplePersistentVersions.add(non_persistentVersion_2);
    //
    representationsListWithZeroPersistentVersions.add(non_persistentVersion_1);
    representationsListWithZeroPersistentVersions.add(non_persistentVersion_2);
  }

  @Test
  public void shouldReturnNullForEmptyList() {
    Representation selectedRepresentation = representationSelector.select(emptyRepresentationsList);
    Assert.assertTrue(selectedRepresentation == null);
  }

  @Test
  public void shouldReturnNullForListWithoutPersistentRepresentations() {
    Representation selectedRepresentation = representationSelector.select(representationsListWithZeroPersistentVersions);
    Assert.assertTrue(selectedRepresentation == null);
  }

  @Test
  public void shouldReturnLatestRepresentationVersion() {
    Representation selectedRepresentation = representationSelector.select(representationsListWithOnePersistentVersion);
    Assert.assertFalse(selectedRepresentation == null);
  }

  @Test
  public void shouldReturnLatestRepresentationVersion_1() {
    Representation selectedRepresentation = representationSelector.select(representationsListWithMultiplePersistentVersions);
    Assert.assertFalse(selectedRepresentation == null);
    Assert.assertTrue(selectedRepresentation.getRepresentationName().equals("name2"));
  }

}
