package eu.europeana.cloud.migrator.provider;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.migrator.ResourceMigrator;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by helin on 2015-12-23.
 */
public class Cleaner {

  private static final Logger logger = LoggerFactory.getLogger(Cleaner.class);

  public void clean(String providerId, RecordServiceClient mcs, UISClient uis) {
    try {
      for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + ResourceMigrator.TEXT_EXTENSION),
          StandardCharsets.UTF_8)) {
        StringTokenizer st = new StringTokenizer(line, ";");
        if (st.hasMoreTokens()) {
          st.nextToken();
        }
        String url = st.nextToken();
        int pos = url.indexOf("/records/");
        if (pos > -1) {
          String id = url.substring(pos + "/records/".length());
          id = id.substring(0, id.indexOf("/"));
          mcs.deleteRecord(id);
        }
      }
    } catch (IOException | MCSException e) {
      logger.error("Error while cleaning ", e);
    }

  }


  public void cleanRecords(String providerId, RecordServiceClient mcs, UISClient uis) {
    try {
      for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + "_ids.txt"),
          StandardCharsets.UTF_8)) {
        String id = line.trim();
        logger.info("Cleaning record: {}", id);
        mcs.deleteRecord(id);
      }
    } catch (IOException | MCSException e) {
      logger.error("Error while cleaning records ", e);
    }

  }
}
