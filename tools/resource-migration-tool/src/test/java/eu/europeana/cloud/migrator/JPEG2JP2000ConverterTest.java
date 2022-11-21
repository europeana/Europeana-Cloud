package eu.europeana.cloud.migrator;

import eu.europeana.cloud.migrator.processing.JPEG2JP2000Converter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JPEG2JP2000ConverterTest {

  private static final String LOCATION = "file:///$1/test4/DIG";

  private static final String FILE = "DIG18000-18999/DIG18400-18499/hm_dig18485.jpg";

  private static final String CONFIG_FILE = "file:///$1/processing.properties";

  private String resDir;

  private JPEG2JP2000Converter converter;

  @Before
  public void setUp() throws Exception {
    resDir = Paths.get(Paths.get(".").toAbsolutePath().normalize().toString(), "src/test/resources").toAbsolutePath().normalize()
                  .toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    System.out.println(resDir);
    converter = new JPEG2JP2000Converter(Paths.get(new URI(CONFIG_FILE.replace("$1", resDir))).toAbsolutePath().toString()
                                              .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR));
  }

  @Ignore // test require ImageMagic and Kakadu (kdu_compress).
  @Test
  public void shouldConvertFile() throws Exception {
    URI toConvert = new URI(LOCATION.replace("$1", resDir) + "/" + FILE);
    System.out.println(toConvert.toURL().toString());
    File jp2 = converter.process(toConvert);
    assertNotNull(jp2);
    assertTrue(jp2.length() > 0);
    jp2.deleteOnExit();
  }
}
