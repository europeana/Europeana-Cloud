package eu.europeana.cloud.api;

import eu.europeana.cloud.data.RevisionInformation;
import java.io.IOException;
import java.util.List;

/**
 * Created by Tarek on 7/15/2019.
 */
public interface RevisionsReader {

  List<RevisionInformation> getRevisionsInformation(String filePath) throws IOException;
}
