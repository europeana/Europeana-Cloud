package eu.europeana.cloud.migrator.processing;

import eu.europeana.cloud.migrator.ResourceMigrator;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JPEG2JP2000Converter
    extends CommandExecutor
    implements FileProcessor {

  private static final Logger logger = LoggerFactory.getLogger(JPEG2JP2000Converter.class);

  private static final int JPEG_TO_TIFF_COMMAND_ID = 0;

  private static final int TIFF_TO_JP2000_COMMAND_ID = 1;

  private static final String TIFF_EXTENSION = ".tiff";

  private static final String JP2000_EXTENSION = ".jp2";

  public JPEG2JP2000Converter(String configFile) {
    super(configFile);
  }

  @Override
  public File process(URI fileURI) {
    File outFolder = new File(workingDirectory);
    File tiff = null;
    File jp2 = null;

    try {
      // run jpeg to tiff conversion
      tiff = File.createTempFile("convert", TIFF_EXTENSION, outFolder);
      List<String> parameters = new ArrayList<String>();
      parameters.add(Paths.get(fileURI).toAbsolutePath().toString()
                          .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR));
      parameters.add(tiff.getAbsolutePath());

      if (!runCommand(JPEG_TO_TIFF_COMMAND_ID, parameters).getResult()) {
        logger.error("Error while converting jpeg to tiff.");
        return null;
      }

      // run tiff to jp2 conversion
      jp2 = File.createTempFile("convert", JP2000_EXTENSION, outFolder);
      parameters.clear();
      parameters.add(tiff.getAbsolutePath());
      parameters.add(jp2.getAbsolutePath());

      if (!runCommand(TIFF_TO_JP2000_COMMAND_ID, parameters).getResult()) {
        logger.error("Error while converting tiff to jp2000.");
        if (jp2 != null) {
          jp2.delete();
        }
        return null;
      }
    } catch (IOException e) {
      logger.error("Problem while converting from JPEG to JP2000.", e);
    } finally {
      if (tiff != null) {
        tiff.delete();
      }
    }
    return jp2;
  }

}
