package eu.europeana.cloud.mcs.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.URI;
import java.util.List;

public class TestUtils {

  public static Representation parseRepresentationFromUri(URI uri) {
    Representation representation = new Representation();

    String[] elements = uri.getRawPath().split("/");
    representation.setVersion(elements[elements.length - 1]);
    representation.setRepresentationName(elements[elements.length - 3]);
    representation.setCloudId(elements[elements.length - 5]);

    return representation;
  }


  public static void assertCorrectlyCreatedRepresentation(RecordServiceClient instance, URI uri, String providerId,
      String cloudId, String representationName)
      throws MCSException {
    assertNotNull(uri);
    Representation representationUri = parseRepresentationFromUri(uri);

    assertEquals(cloudId, representationUri.getCloudId());
    assertEquals(representationName, representationUri.getRepresentationName());

    //get representation and check
    Representation representation = instance.getRepresentation(cloudId, representationName,
        representationUri.getVersion());
    assertNotNull(representation);
    assertEquals(cloudId, representation.getCloudId());
    assertEquals(representationName, representation.getRepresentationName());
    assertEquals(providerId, representation.getDataProvider());
    assertEquals(representationUri.getVersion(), representation.getVersion());
  }


  public static void assertSameFiles(Representation rep1, Representation rep2) {
    List<File> files1 = rep1.getFiles();
    List<File> files2 = rep2.getFiles();

    assertNotNull(files1);
    assertNotNull(files2);

    assertEquals(files1.size(), files2.size());

    for (int i = 0; i < files1.size(); i++) {
      assertEquals(files1.get(i).getMd5(), files2.get(i).getMd5());
      assertEquals(files1.get(i).getContentLength(), files2.get(i).getContentLength());
      assertEquals(files1.get(i).getMimeType(), files2.get(i).getMimeType());
    }
  }


  public static Representation obtainRepresentationFromURI(RecordServiceClient instance, URI uri)
      throws MCSException {
    Representation data = parseRepresentationFromUri(uri);
    return instance.getRepresentation(data.getCloudId(), data.getRepresentationName(), data.getVersion());
  }

  public static int howManyThisRepresentationVersion(DataSetServiceClient instance, String providerId, String dataSetId,
      String representationName, String versionId) throws MCSException {
    List<Representation> result = instance.getDataSetRepresentations(providerId, dataSetId, false).getResults();
    int found = 0;
    for (Representation r : result) {
      if (r.getRepresentationName().equals(representationName)) {
        if (versionId != null) {
          assertEquals(r.getVersion(), versionId);
        }
        found++;
      }
    }
    return found;
  }
}
