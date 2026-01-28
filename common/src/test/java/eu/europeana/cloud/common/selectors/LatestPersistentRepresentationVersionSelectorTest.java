package eu.europeana.cloud.common.selectors;

import eu.europeana.cloud.common.model.Representation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LatestPersistentRepresentationVersionSelectorTest {


    RepresentationSelector representationSelector = new LatestPersistentRepresentationVersionSelector();
    List<Representation> emptyRepresentationsList = Collections.emptyList();
    List<Representation> representationsListWithZeroPersistentVersions = new ArrayList<>(2);
    List<Representation> representationsListWithOnePersistentVersion = new ArrayList<>(3);
    List<Representation> representationsListWithMultiplePersistentVersions = new ArrayList<>(4);

    @BeforeEach
    void prepare() {
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
    void shouldReturnNullForEmptyList() {
        Representation selectedRepresentation = representationSelector.select(emptyRepresentationsList);
        assertTrue(selectedRepresentation == null);
    }

    @Test
    void shouldReturnNullForListWithoutPersistentRepresentations() {
        Representation selectedRepresentation = representationSelector.select(representationsListWithZeroPersistentVersions);
        assertTrue(selectedRepresentation == null);
    }

    @Test
    void shouldReturnLatestRepresentationVersion() {
        Representation selectedRepresentation = representationSelector.select(representationsListWithOnePersistentVersion);
        assertFalse(selectedRepresentation == null);
    }

    @Test
    void shouldReturnLatestRepresentationVersion_1() {
        Representation selectedRepresentation = representationSelector.select(representationsListWithMultiplePersistentVersions);
        assertFalse(selectedRepresentation == null);
        assertTrue(selectedRepresentation.getRepresentationName().equals("name2"));
    }

}
