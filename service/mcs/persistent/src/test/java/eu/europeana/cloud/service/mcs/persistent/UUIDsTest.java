package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.utils.UUIDs;
import com.eaio.uuid.UUID;
import org.junit.Test;

public class UUIDsTest {

    @Test
    public void test(){

        UUID uuid = new UUID();
        System.out.println(""+uuid);
     //   UUIDs.unixTimestamp(uuid);
    }
}
