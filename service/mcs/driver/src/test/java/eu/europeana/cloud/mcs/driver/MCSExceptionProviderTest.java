package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSet;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import junitparams.JUnitParamsRunner;
import static junitparams.JUnitParamsRunner.$;
import junitparams.Parameters;
import static org.hamcrest.Matchers.is;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class MCSExceptionProviderTest {

    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionWhenNullErrorInfoPassed() {
        ErrorInfo errorInfo = null;
        MCSExceptionProvider.generateException(errorInfo);
    }


    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionWhenUnknownErrorInfoCodePassed() {
        ErrorInfo errorInfo = new ErrorInfo("THIS_IS_REALLY_WRONG_CODE", null);
        MCSExceptionProvider.generateException(errorInfo);
    }


    @SuppressWarnings("unused")
    private Object[] statusCodes() {
        return $(
            $(new CannotModifyPersistentRepresentationException(),
                McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString()),
            $(new DataSetAlreadyExistsException(), McsErrorCode.DATASET_ALREADY_EXISTS.toString()),
            $(new DataSetNotExistsException(), McsErrorCode.DATASET_NOT_EXISTS.toString()),
            $(new FileAlreadyExistsException(), McsErrorCode.FILE_ALREADY_EXISTS.toString()),
            $(new FileNotExistsException(), McsErrorCode.FILE_NOT_EXISTS.toString()),
            $(new ProviderNotExistsException(), McsErrorCode.PROVIDER_NOT_EXISTS.toString()),
            $(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()),
            $(new VersionNotExistsException(), McsErrorCode.VERSION_NOT_EXISTS.toString()),
            $(new FileContentHashMismatchException(), McsErrorCode.FILE_CONTENT_HASH_MISMATCH.toString()),
            $(new RepresentationAlreadyInSet(), McsErrorCode.REPRESENTATION_ALREADY_IN_SET.toString()),
            $(new CannotPersistEmptyRepresentationException(),
                McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION.toString()),
            $(new WrongContentRangeException(), McsErrorCode.WRONG_CONTENT_RANGE.toString())
        //
        );
    }


    @Test
    @Parameters(method = "statusCodes")
    public void shouldReturnCorrectException(Throwable ex, String errorCode) {
        ErrorInfo errorInfo = new ErrorInfo(errorCode, "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        Assert.assertEquals(ex.getClass(), exception.getClass());
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    /*
        This one is separate becouse it returns different details message...
    */
    @Test
    public void shouldReturnRecordNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.RECORD_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof RecordNotExistsException);
        assertThat(exception.getMessage(), is("There is no record with provided global id: " + errorInfo.getDetails()));
    }


    @Test(expected = DriverException.class)
    public void shouldThrowDriverException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.toString(), "details");
        MCSExceptionProvider.generateException(errorInfo);
    }
}
