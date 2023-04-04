package eu.europeana.cloud.service.mcs.persistent.swift;

import eu.europeana.cloud.service.mcs.persistent.context.SwiftTestContext;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author olanowak
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SwiftTestContext.class})
public class SwiftContentDAOTest extends ContentDAOTest {

}
