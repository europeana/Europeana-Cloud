package eu.europeana.cloud.service.mcs.persistent.s3;

import eu.europeana.cloud.service.mcs.persistent.context.S3TestContext;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author olanowak
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {S3TestContext.class})
public class S3ContentDAOTest extends ContentDAOTest {

}
