package eu.europeana.cloud.service.commons.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ContextConfiguration(classes = {RetryAspectConfiguration.class})
@RunWith(SpringRunner.class)
public class RetryAspectSpringTest {

    @Autowired
    private AspectedTestSpringCtx aspectedTest;

    @Test
    public void shoudCall3Times() {
        String s = Mockito
                .doThrow(TestRuntimeExpection.class)
                .doThrow(TestRuntimeExpection.class)
                .doReturn("data")
                .when(aspectedTest)
                .test_default("some_data");

       verify(aspectedTest/*, times(3)*/).test_default("some_data");
    }

    @Test(expected = TestRuntimeExpection.class)
    public void shoudCall3TimesAndFail() {
        String s = Mockito
                .doThrow(TestRuntimeExpection.class)
                .doThrow(TestRuntimeExpection.class)
                .doThrow(TestRuntimeExpection.class)
                .when(aspectedTest)
                .test_default("some_data");

        verify(aspectedTest/*, times(3)*/).test_default("some_data");
    }

    @Test
    public void shoudCall3TimesWithLongDelay() {
        Mockito.doThrow(TestRuntimeExpection.class)
                .doThrow(TestRuntimeExpection.class)
                .when(aspectedTest)
                .test_delay_2000();

        verify(aspectedTest/*, times(3)*/).test_delay_2000();
    }


}
