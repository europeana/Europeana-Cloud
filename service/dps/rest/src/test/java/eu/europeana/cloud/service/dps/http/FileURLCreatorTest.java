package eu.europeana.cloud.service.dps.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.UnsupportedEncodingException;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

class FileURLCreatorTest {

    FileURLCreator creator = new FileURLCreator("http://127.0.0.1");

    @BeforeEach
    void init() {
        ReflectionTestUtils.setField(creator, "harvestingTasksDir", "/var/www/http_harvest");
    }

    @Test
    void shouldEncodeCharacters_1() throws UnsupportedEncodingException {
        String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "&first&", "second", "third"));
        assertThat(result, not(containsString("&")));
        assertThat(result, containsString("http://127.0.0.1/http_harvest/%26first%26/second/third"));
    }

    @Test
    void shouldEncodeCharacters_2() throws UnsupportedEncodingException {
        String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "#second#", "third"));
        assertThat(result, not(containsString("#")));
        assertThat(result, containsString("http://127.0.0.1/http_harvest/first/%23second%23/third"));
    }

    @Test
    void shouldEncodeCharacters_3() throws UnsupportedEncodingException {
        String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "second", "?third"));
        assertThat(result, not(containsString("?")));
        assertThat(result, containsString("http://127.0.0.1/http_harvest/first/second/%3Fthird"));
    }

    @Test
    void shouldEncodeCharacters_4() throws UnsupportedEncodingException {
        String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first@", "!second", "third"));
        assertThat(result, not(containsString("@")));
        assertThat(result, not(containsString("!")));
        assertThat(result, containsString("http://127.0.0.1/http_harvest/first%40/%21second/third"));
    }

    @Test
    void shouldEncodeCharacters_5() throws UnsupportedEncodingException {
        String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "second", "fileName.xml"));
        assertThat(result, not(containsString("@")));
        assertThat(result, not(containsString("!")));
        assertThat(result, containsString("http://127.0.0.1/http_harvest/first/second/fileName.xml"));
    }

    @Test
    void shouldEncodeCharacters_6() throws UnsupportedEncodingException {
        String result = creator.generateUrlFor(Path.of("/", "var", "www", "http_harvest", "first", "second", "fileName#$%.xml"));
        assertThat(result, not(containsString("@")));
        assertThat(result, not(containsString("!")));
        assertThat(result, containsString("http://127.0.0.1/http_harvest/first/second/fileName%23%24%25.xml"));
    }
}