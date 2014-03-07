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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

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


    @Test
    public void shouldReturnCannotModifyPersistentRepresentationException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof CannotModifyPersistentRepresentationException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnDataSetAlreadyExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.DATASET_ALREADY_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof DataSetAlreadyExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnDataSetNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.DATASET_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof DataSetNotExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnFileAlreadyExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.FILE_ALREADY_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof FileAlreadyExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnFileNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.FILE_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof FileNotExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnProviderNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.PROVIDER_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof ProviderNotExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnRecordNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.RECORD_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof RecordNotExistsException);
        assertThat(exception.getMessage(), is("There is no record with provided global id: " + errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnRepresentationNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof RepresentationNotExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnVersionNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.VERSION_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof VersionNotExistsException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnFileContentHashMismatchException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.FILE_CONTENT_HASH_MISMATCH.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof FileContentHashMismatchException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnRepresentationAlreadyInSet() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.REPRESENTATION_ALREADY_IN_SET.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof RepresentationAlreadyInSet);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnCannotPersistEmptyRepresentationException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof CannotPersistEmptyRepresentationException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test
    public void shouldReturnWrongContentRangeException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.WRONG_CONTENT_RANGE.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof WrongContentRangeException);
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    @Test(expected = DriverException.class)
    public void shouldReturnDriverException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.toString(), "details");
        MCSExceptionProvider.generateException(errorInfo);
    }
}
