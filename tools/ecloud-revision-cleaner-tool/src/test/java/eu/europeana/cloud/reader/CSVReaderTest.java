package eu.europeana.cloud.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import eu.europeana.cloud.data.RevisionInformation;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

public class CSVReaderTest {

  private CSVReader csvReader = new CSVReader();

  @Test
  public void shouldReadAndReturnTaskIdsForCSVFile() throws IOException {
    List<RevisionInformation> taskIds = csvReader.getRevisionsInformation(
        Paths.get("src/test/resources/revisions.csv").toString());
    assertNotNull(taskIds);
    assertEquals(8, taskIds.size());
  }
}