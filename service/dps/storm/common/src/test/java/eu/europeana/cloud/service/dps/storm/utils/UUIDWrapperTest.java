package eu.europeana.cloud.service.dps.storm.utils;

import org.junit.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class UUIDWrapperTest {

  @Test
  public void shouldAlwaysGenerateTheSameUUID() {
    Instant date = Instant.ofEpochSecond(100);
    UUID uuid = UUIDWrapper.generateRepresentationVersion(date, "record_id");
    for (int i = 0; i < 100; i++) {
      UUID uuid_1 = UUIDWrapper.generateRepresentationVersion(date, "record_id");
      assertEquals(uuid_1, uuid);
    }
  }

  @Test
  public void shouldGenerateDifferentUUIDsForDifferentDates() {
    UUID uuid_1 = UUIDWrapper.generateRepresentationVersion(Instant.ofEpochSecond(100), "record_id");
    UUID uuid_2 = UUIDWrapper.generateRepresentationVersion(Instant.ofEpochSecond(200), "record_id");
    UUID uuid_3 = UUIDWrapper.generateRepresentationVersion(Instant.ofEpochSecond(300), "record_id");
    UUID uuid_4 = UUIDWrapper.generateRepresentationVersion(Instant.ofEpochSecond(400), "record_id");
    assertNotEquals(uuid_1, uuid_2);
    assertNotEquals(uuid_1, uuid_3);
    assertNotEquals(uuid_1, uuid_4);

    assertNotEquals(uuid_2, uuid_3);
    assertNotEquals(uuid_2, uuid_4);

    assertNotEquals(uuid_3, uuid_4);
  }

  @Test
  public void shouldGenerateDifferentUUIDsForDifferentRecords() {
    Instant date = Instant.ofEpochSecond(100);
    UUID uuid_1 = UUIDWrapper.generateRepresentationVersion(date, "record_id");
    UUID uuid_2 = UUIDWrapper.generateRepresentationVersion(date, "record_id_1");
    UUID uuid_3 = UUIDWrapper.generateRepresentationVersion(date, "record_id_2");
    UUID uuid_4 = UUIDWrapper.generateRepresentationVersion(date, "record_id_3");
    assertNotEquals(uuid_1, uuid_2);
    assertNotEquals(uuid_1, uuid_3);
    assertNotEquals(uuid_1, uuid_4);

    assertNotEquals(uuid_2, uuid_3);
    assertNotEquals(uuid_2, uuid_4);

    assertNotEquals(uuid_3, uuid_4);
  }

  @Test
  public void shouldGenerateProperUUIDsForSomeRandomData() {
    Instant date = Instant.now();

    UUID uuid_1 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/MJBADW4HV7BTJHA4QBUOVARRUF2HKRN5YTPCCIYJKOSJYZRXBDCQ/representations/metadataRecord/versions/39f7a4c0-e4b0-11eb-835c-fa163e64bb83/files/8918dc61-4236-47f4-be9a-5b4f6cd228c4");
    UUID uuid_2 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/PVABNBFCI3FUDA2L4WEYSIYW2DIXX3MWYA66CAMRUSEDWQHZFLVA/representations/metadataRecord/versions/39d72470-e4b0-11eb-a1d9-fa163e1e1541/files/4576f18d-fc82-4c20-9b51-65f8e152445f");
    UUID uuid_3 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/RQHT4L72CXTVQVLX5Y22UCZVZJZN5ZZMDYRFFSTCIKGHT2W4VMHA/representations/metadataRecord/versions/3a3d8760-e4b0-11eb-835c-fa163e64bb83/files/5a336727-ad4a-4f01-bc75-7ac4aabf609c");
    UUID uuid_4 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/NRTYDREPVZD4DMQQRZVSIR6KUQ4FA76IAARWP6KNA6X2XVQIYIPA/representations/metadataRecord/versions/34338220-e4b0-11eb-a1d9-fa163e1e1541/files/0325dce4-3eba-4c08-90c1-15be8ef9e77d");
    UUID uuid_5 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/V6AAELJ26EQDUI7UIQJ2NXDY7TDGZH7A363DHXGZQFCORA6YPHHQ/representations/metadataRecord/versions/39f0ee00-e4b0-11eb-a1d9-fa163e1e1541/files/3b81f124-b6d3-4540-9c83-6fd0c37fe8b5");
    UUID uuid_6 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/WPXQP5ANYSTI6RUIIQTIOX3DXR2P62VE2WDVJ6C6MCCGLJ53GLWQ/representations/metadataRecord/versions/399bc920-e4b0-11eb-835c-fa163e64bb83/files/c66a8a58-ec48-4849-99f9-8a3130d2d7f2");
    UUID uuid_7 = UUIDWrapper.generateRepresentationVersion(date,
        "https://test.ecloud.psnc.pl/api/records/UN2YKV6CM3SAN4IVQL62OGJVYTMYSH3UOICAZ72SL4LSO5DMPRJA/representations/metadataRecord/versions/395d1270-e4b0-11eb-a1d9-fa163e1e1541/files/a9640b30-110f-49c9-8ced-c3de9eca6ad4");

    assertNotEquals(uuid_1, uuid_2);
    assertNotEquals(uuid_1, uuid_3);
    assertNotEquals(uuid_1, uuid_4);
    assertNotEquals(uuid_1, uuid_5);
    assertNotEquals(uuid_1, uuid_6);
    assertNotEquals(uuid_1, uuid_7);

    assertNotEquals(uuid_2, uuid_3);
    assertNotEquals(uuid_2, uuid_4);
    assertNotEquals(uuid_2, uuid_5);
    assertNotEquals(uuid_2, uuid_6);
    assertNotEquals(uuid_2, uuid_7);

    assertNotEquals(uuid_3, uuid_4);
    assertNotEquals(uuid_3, uuid_5);
    assertNotEquals(uuid_3, uuid_6);
    assertNotEquals(uuid_3, uuid_7);

    assertNotEquals(uuid_4, uuid_5);
    assertNotEquals(uuid_4, uuid_6);
    assertNotEquals(uuid_4, uuid_7);

    assertNotEquals(uuid_5, uuid_6);
    assertNotEquals(uuid_5, uuid_7);

    assertNotEquals(uuid_6, uuid_7);
  }
}