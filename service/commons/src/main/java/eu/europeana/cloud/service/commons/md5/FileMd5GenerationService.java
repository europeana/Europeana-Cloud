package eu.europeana.cloud.service.commons.md5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Generates md5 hash from the provided file.
 * Result is in HEX.
 */
public class FileMd5GenerationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileMd5GenerationService.class);

    private FileMd5GenerationService() {
    }

    public static String generate(Path file) throws IOException {
        try (var is = Files.newInputStream(file)) {
            return org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
        }
    }

    public static String generate(byte[] fileBytes) {
        try (var is = new ByteArrayInputStream(fileBytes)) {
            return org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
        } catch (IOException e) {
            LOGGER.error("Unable to generate md5", e);
            return "";
        }


    }


}
