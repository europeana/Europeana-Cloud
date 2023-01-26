package eu.europeana.cloud.service.dps.storm.utils;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileDataCheckerTests {

    @Test
    public void shouldProperlyDetectEmptyString() throws IOException {
        byte[] testValue = "".getBytes(StandardCharsets.UTF_8);
        assertTrue(FileDataChecker.isFileDataNullOrBlank(testValue));
    }

    @Test
    public void shouldProperlyDetectBlankString() throws IOException {
        byte[] tabulatorTest = "         ".getBytes(StandardCharsets.UTF_8);
        byte[] spaceTest = "                ".getBytes(StandardCharsets.UTF_8);
        byte[] newLineTest = "\n\n\n\n\n".getBytes(StandardCharsets.UTF_8);
        assertTrue(FileDataChecker.isFileDataNullOrBlank(tabulatorTest));
        assertTrue(FileDataChecker.isFileDataNullOrBlank(spaceTest));
        assertTrue(FileDataChecker.isFileDataNullOrBlank(newLineTest));
    }

    @Test
    public void shouldProperlyHandleNullByteArray() throws IOException {
        assertTrue(FileDataChecker.isFileDataNullOrBlank(null));
    }

    @Test
    public void shouldProperlyHandleFilledString() throws IOException {
        byte[] testString = "  test  String  ".getBytes(StandardCharsets.UTF_8);
        byte[] testString2 = "  test  String2       ".getBytes(StandardCharsets.UTF_8);
        assertFalse(FileDataChecker.isFileDataNullOrBlank(testString));
        assertFalse(FileDataChecker.isFileDataNullOrBlank(testString2));
    }
}
