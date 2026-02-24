package eu.europeana.cloud.service.dps.test;

import static eu.europeana.cloud.service.dps.test.TestConstants.CLOUD_ID;
import static eu.europeana.cloud.service.dps.test.TestConstants.CLOUD_ID2;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Tarek on 3/13/2017.
 */
public class TestHelper {

  public final List<CloudTagsResponse> prepareCloudTagsResponsesList() {
    List<CloudTagsResponse> cloudTagsResponseList = new ArrayList<>(2);
    CloudTagsResponse cloudTagsResponseResponse1 = new CloudTagsResponse(SOURCE + CLOUD_ID, false);
    CloudTagsResponse cloudTagsResponseResponse12 = new CloudTagsResponse(SOURCE + CLOUD_ID2, false);
    cloudTagsResponseList.add(cloudTagsResponseResponse1);
    cloudTagsResponseList.add(cloudTagsResponseResponse12);
    return cloudTagsResponseList;
  }

  public final List<CloudTagsResponse> prepareCloudTagsResponsesList(int n) {
    List<CloudTagsResponse> cloudTagsResponseList = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      CloudTagsResponse cloudTagsResponseResponse1 = new CloudTagsResponse(SOURCE + CLOUD_ID + i, false);
      cloudTagsResponseList.add(cloudTagsResponseResponse1);
    }
    return cloudTagsResponseList;
  }

  public final Representation prepareRepresentation(String cloudId, String representationName, String version,
      String fileUrl, String dataProvider, boolean persistent,
      Date creationDate, String datasetId) throws URISyntaxException {
    return prepareRepresentationWithMultipleFiles(cloudId, representationName, version, fileUrl, dataProvider, persistent,
        creationDate, 1, datasetId);
  }

  public final Representation prepareRepresentationWithMultipleFiles(String cloudId, String representationName,
      String version, String fileUrl, String dataProvider,
      boolean persistent, Date creationDate,
      int fileCount, String datasetId) throws URISyntaxException {
    List<File> files = new ArrayList<>(fileCount);
    List<Revision> revisions = new ArrayList<>(0);
    for (int i = 0; i < fileCount; i++) {
      files.add(new File("fileName", "text/plain", "md5", "1", 5, new URI(fileUrl)));
    }
    return new Representation(cloudId, representationName, version, new URI(fileUrl), new URI(fileUrl), dataProvider, files,
        revisions, persistent, creationDate, datasetId, false);
  }

}
