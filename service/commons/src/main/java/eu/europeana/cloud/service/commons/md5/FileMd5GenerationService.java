package eu.europeana.cloud.service.commons.md5;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Generates md5 hash from the provided file.
 * Result is in HEX.
 */
public class FileMd5GenerationService {

    private FileMd5GenerationService() {
    }

    public static UUID generate(Path filePath) throws IOException {
        return generate(Files.readAllBytes(filePath));
    }

    public static UUID generate(byte[] fileBytes) {
        return UUID.nameUUIDFromBytes(fileBytes);
    }
}
