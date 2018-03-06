package eu.europeana.cloud.service.dps.test;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;

/**
 * Created by Tarek on 3/13/2017.
 */
public class TestHelper {
    public List<CloudTagsResponse> prepareCloudTagsResponsesList() {
        List<CloudTagsResponse> CloudTagsResponseList = new ArrayList<>();
        CloudTagsResponse cloudTagsResponseResponse1 = new CloudTagsResponse(SOURCE + CLOUD_ID, true, false, false);
        CloudTagsResponse cloudTagsResponseResponse12 = new CloudTagsResponse(SOURCE + CLOUD_ID2, true, false, false);
        CloudTagsResponseList.add(cloudTagsResponseResponse1);
        CloudTagsResponseList.add(cloudTagsResponseResponse12);
        return CloudTagsResponseList;
    }

    public List<CloudIdAndTimestampResponse> prepareCloudIdAndTimestampResponseList(Date date) {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = new ArrayList<>();
        CloudIdAndTimestampResponse cloudIdAndTimestampResponse = new CloudIdAndTimestampResponse(SOURCE + CLOUD_ID, date);
        CloudIdAndTimestampResponse cloudIdAndTimestampResponse2 = new CloudIdAndTimestampResponse(SOURCE + CLOUD_ID2, date);
        cloudIdAndTimestampResponseList.add(cloudIdAndTimestampResponse);
        cloudIdAndTimestampResponseList.add(cloudIdAndTimestampResponse2);
        return cloudIdAndTimestampResponseList;
    }


    public Representation prepareRepresentation(String cloudId, String representationName, String version, String fileUrl,
                                                String dataProvider, boolean persistent, Date creationDate) throws URISyntaxException {
        List<File> files = new ArrayList<>();
        List<Revision> revisions = new ArrayList<>();
        files.add(new File("fileName", "text/plain", "md5", "1", 5, new URI(fileUrl)));
        Representation representation = new Representation(cloudId, representationName, version, new URI(fileUrl), new URI(fileUrl), dataProvider, files, revisions, persistent, creationDate);
        return representation;
    }

}
