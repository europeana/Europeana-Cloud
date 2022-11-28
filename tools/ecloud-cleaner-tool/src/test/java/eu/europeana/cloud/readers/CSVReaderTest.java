package eu.europeana.cloud.readers;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

public class CSVReaderTest {

  private final CommaSeparatorReaderImpl csvReader = new CommaSeparatorReaderImpl();

  @Test
  public void shouldReadAndReturnTaskIdsForCSVFile() throws IOException {
    List<String> taskIds = csvReader.getTaskIds(Paths.get("src/test/resources/taskIds.csv").toString());
    assertNotNull(taskIds);
    assertEquals(6, taskIds.size());
  }

  @Test
  public void shouldReadAndReturnTaskIdsForTXTFile() throws IOException {
    List<String> taskIds = csvReader.getTaskIds(Paths.get("src/test/resources/taskIds.txt").toString());
    assertNotNull(taskIds);
    assertEquals(6, taskIds.size());
  }


}