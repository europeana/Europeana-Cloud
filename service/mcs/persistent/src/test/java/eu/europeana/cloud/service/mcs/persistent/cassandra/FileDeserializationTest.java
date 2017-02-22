package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.google.gson.Gson;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.Storage;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author krystian.
 */
public class FileDeserializationTest {
    private final Gson gson = new Gson();

    @Test
    public void shouldProperlyDeserializeJsonWithoutStorageEnumToDefaultValue() {
        //given
        String json = "{" +
                "\"fileName\":\"name\"," +
                "\"mimeType\":\"application/xml\"," +
                "\"md5\":\"someMd5\"," +
                "\"date\":\"2016-04-20T00:27:21.866+02:00\"," +
                "\"contentLength\":1111}";

        //when
        File file = gson.fromJson(json, File.class);

        //then
        assertThat(file.getFileStorage(), is(Storage.OBJECT_STORAGE));
    }
}