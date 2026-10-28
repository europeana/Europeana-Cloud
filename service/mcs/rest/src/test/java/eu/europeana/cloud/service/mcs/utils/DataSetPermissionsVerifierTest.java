package eu.europeana.cloud.service.mcs.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;

@ExtendWith(MockitoExtension.class)
class DataSetPermissionsVerifierTest {
  private static final String CLOUD_ID = "cloud-1";
  private static final String REPRESENTATION_NAME = "representation-1";
  private static final String VERSION = "550e8400-e29b-11d4-a716-446655440000";

  @Mock
  private PermissionEvaluator permissionEvaluator;
  @Mock
  private RecordService recordService;


  @InjectMocks
  private DataSetPermissionsVerifier verifier;

  @Test
  void shouldWriteRepresentationFromDatasetWithPermission() throws Exception {
    // given
    Representation input = Representation.fromFields(
        CLOUD_ID, REPRESENTATION_NAME, VERSION);

    Representation representationFromDb = Representation.fromFields(
        CLOUD_ID, REPRESENTATION_NAME, VERSION);

    representationFromDb.setDatasetIds(Set.of("dataset-1"));
    representationFromDb.setDataProvider("provider-1");

    when(recordService.getRepresentation(CLOUD_ID, REPRESENTATION_NAME, VERSION))
        .thenReturn(representationFromDb);

    when(permissionEvaluator.hasPermission(
        nullable(Authentication.class),
        eq("dataset-1/provider-1"),
        eq(DataSet.class.getName()),
        eq(Permission.WRITE.getValue()))
    ).thenReturn(true);

    // when
    boolean result = verifier.hasWritePermissionFor(input);

    // then
    assertTrue(result);
  }

  @Test
  void shouldNotWriteRepresentationifNotAllItsDatasetsHasPermission() throws Exception {
    // given
    Representation input = Representation.fromFields(
        CLOUD_ID, REPRESENTATION_NAME, VERSION);

    Representation representationFromDb = Representation.fromFields(
        CLOUD_ID, REPRESENTATION_NAME, VERSION);

    representationFromDb.setDatasetIds(Set.of("dataset-1","dataset-2"));
    representationFromDb.setDataProvider("provider-1");

    when(recordService.getRepresentation(CLOUD_ID, REPRESENTATION_NAME, VERSION))
        .thenReturn(representationFromDb);

    lenient().when(permissionEvaluator.hasPermission(
        any(Authentication.class),
        eq("dataset-1/provider-1"),
        eq(DataSet.class.getName()),
        eq(Permission.WRITE.getValue()))
    ).thenReturn(true);

    // when
    boolean result = verifier.hasWritePermissionFor(input);

    // then
    assertFalse(result);
  }
}