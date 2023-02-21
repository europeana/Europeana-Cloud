package eu.europeana.cloud.service.dps.http;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class FileURLCreatorTest {

  FileURLCreator creator = new FileURLCreator("http://127.0.0.1");

  @Before
  public void init() {
    ReflectionTestUtils.setField(creator, "harvestingTasksDir", "/var/www/http_harvest");
  }

  @Test
  public void shouldEncodeCharacters_1() throws UnsupportedEncodingException {
    String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "&first&", "second", "third"));
    assertThat(result, not(containsString("&")));
    assertThat(result, containsString("http://127.0.0.1/http_harvest/%26first%26/second/third"));
  }

  @Test
  public void shouldEncodeCharacters_2() throws UnsupportedEncodingException {
    String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "#second#", "third"));
    assertThat(result, not(containsString("#")));
    assertThat(result, containsString("http://127.0.0.1/http_harvest/first/%23second%23/third"));
  }

  @Test
  public void shouldEncodeCharacters_3() throws UnsupportedEncodingException {
    String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "second", "?third"));
    assertThat(result, not(containsString("?")));
    assertThat(result, containsString("http://127.0.0.1/http_harvest/first/second/%3Fthird"));
  }

  @Test
  public void shouldEncodeCharacters_4() throws UnsupportedEncodingException {
    String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first@", "!second", "third"));
    assertThat(result, not(containsString("@")));
    assertThat(result, not(containsString("!")));
    assertThat(result, containsString("http://127.0.0.1/http_harvest/first%40/%21second/third"));
  }

  @Test
  public void shouldEncodeCharacters_5() throws UnsupportedEncodingException {
    String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "second", "fileName.xml"));
    assertThat(result, not(containsString("@")));
    assertThat(result, not(containsString("!")));
    assertThat(result, containsString("http://127.0.0.1/http_harvest/first/second/fileName.xml"));
  }

  @Test
  public void shouldEncodeCharacters_6() throws UnsupportedEncodingException {
    String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "second", "fileName#$%.xml"));
    assertThat(result, not(containsString("@")));
    assertThat(result, not(containsString("!")));
    assertThat(result, containsString("http://127.0.0.1/http_harvest/first/second/fileName%23%24%25.xml"));
  }
}