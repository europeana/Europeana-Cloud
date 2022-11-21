package eu.europeana.cloud.service.mcs.persistent.swift;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author olanowak
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/swiftTestContext.xml"})
public class SwiftContentDAOTest extends ContentDAOTest {

}
