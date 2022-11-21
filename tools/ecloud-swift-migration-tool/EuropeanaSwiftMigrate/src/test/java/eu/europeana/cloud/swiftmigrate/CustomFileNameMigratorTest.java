package eu.europeana.cloud.swiftmigrate;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class CustomFileNameMigratorTest {

  CustomFileNameMigrator migrator;


  @Before
  public void init() {
    migrator = new CustomFileNameMigrator();
  }


  @Test
  public void shouldRetrunNullString() {
    //given
    final String fileName = "";
    //when
    final String output = migrator.nameConversion(fileName);
    //then
    assertEquals(null, output);
  }


  @Test
  public void shouldConvertFileProperly() {
    //given
    final String fileName = "test2|test1|tes2";
    //when
    final String output = migrator.nameConversion(fileName);
    //then
    assertEquals("test2_test1_tes2", output);
  }


  @Test
  public void shouldConvertFileProperly2() {
    //given
    final String fileName = "test2_test1|tes2";
    //when
    final String output = migrator.nameConversion(fileName);
    //then
    assertEquals("test2_test1_tes2", output);
  }
}
