package eu.europeana.cloud.service.dps.storm.utils;

import com.github.f4b6a3.uuid.UuidCreator;
import com.github.f4b6a3.uuid.creator.rfc4122.TimeBasedUuidCreator;
import com.github.f4b6a3.uuid.strategy.NodeIdentifierStrategy;
import com.github.f4b6a3.uuid.util.UuidTime;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.UUID;

/**
 * Wraps all the operations related with UUIDs
 */
public class UUIDWrapper {


  private UUIDWrapper() {
  }

  /**
   * Generates representation version that will be later used for uploading file content to the eCloud by the topologies (usually
   * in the WriteRecordBolt). It will generate version-1 UUID.
   *
   * @param date this date will be inserted to the UUID in the 'time' part of the UUID version 1
   * @param recordId it will be hashed (md5) and inserted to the 'node' part of the UUID
   * @return generated UUID in version 1
   */
  public static UUID generateRepresentationVersion(Instant date, String recordId) {
    NodeIdentifierStrategy nodeIdentifierStrategy = new CustomNodeIdentifierStrategy(recordId);

    TimeBasedUuidCreator uuidCreator = UuidCreator.getTimeBasedCreator()
                                                  .withNodeIdentifierStrategy(nodeIdentifierStrategy)
                                                  .withTimestampStrategy(() -> UuidTime.toTimestamp(date))
                                                  .withClockSequence(0);

    return uuidCreator.create();
  }

  public static String generateRepresentationFileName(String recordId) {
    return UuidCreator.getNameBasedMd5(recordId).toString();
  }

  private static class CustomNodeIdentifierStrategy implements NodeIdentifierStrategy {

    private final String recordId;

    public CustomNodeIdentifierStrategy(String recordId) {
      this.recordId = recordId;
    }

    @Override
    public long getNodeIdentifier() {
      try {
        byte[] md5 = MessageDigest.getInstance("MD5").digest(recordId.getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(md5).getLong();
      } catch (NoSuchAlgorithmException var3) {
        throw new InternalError("MD5 not supported", var3);
      }

    }
  }

}
