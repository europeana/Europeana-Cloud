package eu.europeana.cloud.service.commons.utils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Calendar;

import static org.junit.Assert.assertTrue;

@ContextConfiguration(classes = {RetryAspectConfiguration.class})
@RunWith(SpringRunner.class)
public class RetryAspectSpringTest {

    @Autowired
    private AspectedTestSpringCtx aspectedTest;

    @Before
    public void resetData() {
        aspectedTest.resetAttempts();
    }

    @Test
    public void shoudCallDefault3Times() {
        long startTime = Calendar.getInstance().getTimeInMillis();
        String result = aspectedTest.test_default("Text to process");
        long endTime = Calendar.getInstance().getTimeInMillis();

        assertTrue(result.contains("Text to process"));
        assertTrue(endTime - startTime >= 2*1000);
    }

    @Test
    public void shoudCall10Times() {
        long startTime = Calendar.getInstance().getTimeInMillis();
        aspectedTest.test_delay_500_10();
        long endTime = Calendar.getInstance().getTimeInMillis();

        assertTrue(endTime - startTime >= 9*500);
    }

    @Test(expected = TestRuntimeExpection.class)
    public void shoudCall6TimesAndFail(){
        long startTime = Calendar.getInstance().getTimeInMillis();
        aspectedTest.test_delay_2000_6();
        long endTime = Calendar.getInstance().getTimeInMillis();

        assertTrue(endTime - startTime >= 5*2000);
    }

    @Test
    public void shoudCall4Times() {
        long startTime = Calendar.getInstance().getTimeInMillis();
        aspectedTest.test_delay_3000_4();
        long endTime = Calendar.getInstance().getTimeInMillis();

        assertTrue(endTime - startTime >= 3*3000);
    }
}
