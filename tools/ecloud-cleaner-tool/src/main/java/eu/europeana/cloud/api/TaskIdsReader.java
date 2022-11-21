package eu.europeana.cloud.api;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tarek on 4/17/2019.
 */
public interface TaskIdsReader {

  List<String> getTaskIds(String filePath) throws IOException;
}
