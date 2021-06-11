package eu.europeana.cloud.service.commons.md5;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
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

    public static boolean areEquals(byte[] md5digest, UUID uuid) {
        long msb = 0L;
        long lsb = 0L;

        if(md5digest == null && uuid == null) {
            return true;
        } else if(md5digest == null || uuid == null) {
            return false;
        }

        int i;
        for(i = 0; i < 8; ++i) {
            msb = msb << 8 | (long)(md5digest[i] & 255);
        }

        for(i = 8; i < 16; ++i) {
            lsb = lsb << 8 | (long)(md5digest[i] & 255);
        }

        UUID md5uuid = new UUID(msb, lsb);

        return md5uuid.compareTo(uuid) == 0;
    }

}
