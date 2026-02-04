package eu.europeana.cloud.service.mcs.persistent.s3;

import eu.europeana.cloud.service.mcs.persistent.context.S3TestContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author olanowak
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {S3TestContext.class})
class S3ContentDAOTest extends ContentDAOTest {

}
