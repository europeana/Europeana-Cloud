package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class MCSExceptionProviderTest {

    @Test
    void shouldThrowDriverExceptionWhenNullErrorInfoPassed() {
        ErrorInfo errorInfo = null;
        assertThrows(DriverException.class, () -> MCSExceptionProvider.generateException(errorInfo));
    }


    @Test
    void shouldThrowDriverExceptionWhenUnknownErrorInfoCodePassed() {
        ErrorInfo errorInfo = new ErrorInfo("THIS_IS_REALLY_WRONG_CODE", null);
        assertThrows(DriverException.class, () -> MCSExceptionProvider.generateException(errorInfo));
    }


    @ParameterizedTest
    @MethodSource("statusCodes")
    void shouldReturnCorrectException(Throwable ex, String errorCode) {
        ErrorInfo errorInfo = new ErrorInfo(errorCode, "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertEquals(ex.getClass(), exception.getClass());
        assertThat(exception.getMessage(), is(errorInfo.getDetails()));
    }


    /*
        This one is separate because it returns different details message...
    */
    @Test
    void shouldReturnRecordNotExistsException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.RECORD_NOT_EXISTS.toString(), "details");
        MCSException exception = MCSExceptionProvider.generateException(errorInfo);
        assertTrue(exception instanceof RecordNotExistsException);
        assertThat(exception.getMessage(), is("There is no record with provided global id: " + errorInfo.getDetails()));
    }


    @Test
    void shouldThrowDriverException() {
        ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.toString(), "details");
        assertThrows(DriverException.class, () -> MCSExceptionProvider.generateException(errorInfo));
    }


    private static Stream<Arguments> statusCodes() {
        return Stream.of(
                arguments(new CannotModifyPersistentRepresentationException(),
                        McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString()),
                arguments(new DataSetAlreadyExistsException(),
                        McsErrorCode.DATASET_ALREADY_EXISTS.toString()),
                arguments(new DataSetNotExistsException(),
                        McsErrorCode.DATASET_NOT_EXISTS.toString()),
                arguments(new FileAlreadyExistsException(),
                        McsErrorCode.FILE_ALREADY_EXISTS.toString()),
                arguments(new FileNotExistsException(),
                        McsErrorCode.FILE_NOT_EXISTS.toString()),
                arguments(new ProviderNotExistsException(),
                        McsErrorCode.PROVIDER_NOT_EXISTS.toString()),
                arguments(new RepresentationNotExistsException(),
                        McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()),
                arguments(new FileContentHashMismatchException(),
                        McsErrorCode.FILE_CONTENT_HASH_MISMATCH.toString()),
                arguments(new RepresentationAlreadyInSet(),
                        McsErrorCode.REPRESENTATION_ALREADY_IN_SET.toString()),
                arguments(new CannotPersistEmptyRepresentationException(),
                        McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION.toString()),
                arguments(new WrongContentRangeException(),
                        McsErrorCode.WRONG_CONTENT_RANGE.toString())
        );
    }

}
