package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.service.dps.DpsTask.TaskInput;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FilesUrls implements TaskInput {

  private List<String> fileUrls;

  public FilesUrls(String... files) {
    this.fileUrls = Arrays.asList(files);
  }
}
