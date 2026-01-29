package eu.europeana.cloud.service.dps.storm.utils;


import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileDataCheckerTest {

    @Test
    void shouldProperlyDetectEmptyString() throws IOException {
        byte[] testValue = "".getBytes(StandardCharsets.UTF_8);
        assertTrue(FileDataChecker.isFileDataNullOrBlank(testValue));
    }

    @Test
    void shouldProperlyDetectBlankString() throws IOException {
        byte[] tabulatorTest = "         ".getBytes(StandardCharsets.UTF_8);
        byte[] spaceTest = "                ".getBytes(StandardCharsets.UTF_8);
        byte[] newLineTest = "\n\n\n\n\n".getBytes(StandardCharsets.UTF_8);
        assertTrue(FileDataChecker.isFileDataNullOrBlank(tabulatorTest));
        assertTrue(FileDataChecker.isFileDataNullOrBlank(spaceTest));
        assertTrue(FileDataChecker.isFileDataNullOrBlank(newLineTest));
    }

    @Test
    void shouldProperlyHandleNullByteArray() throws IOException {
        assertTrue(FileDataChecker.isFileDataNullOrBlank(null));
    }

    @Test
    void shouldProperlyHandleFilledString() throws IOException {
        byte[] testString = "  test  String  ".getBytes(StandardCharsets.UTF_8);
        byte[] testString2 = "  test  String2       ".getBytes(StandardCharsets.UTF_8);
        assertFalse(FileDataChecker.isFileDataNullOrBlank(testString));
        assertFalse(FileDataChecker.isFileDataNullOrBlank(testString2));
    }
}
