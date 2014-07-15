package eu.europeana.cloud.service.dls.kafka;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomerWrapperTest {

    @Autowired
    private CustomerWrapper customerWrapper;

    public CustomerWrapperTest() {
	// TODO @krystian
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testInit() {
	assertTrue(true);
    }

    @Test
    public void testDestroy() {
    }

}
