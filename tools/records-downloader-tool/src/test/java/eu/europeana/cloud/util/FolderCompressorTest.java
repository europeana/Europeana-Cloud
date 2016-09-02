package eu.europeana.cloud.util;

import eu.europeana.cloud.TestConstantsHelper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Test;
import org.zeroturnaround.zip.ZipException;

import java.io.*;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FolderCompressorTest implements TestConstantsHelper {
    String folderPath;

    @Test(expected = ZipException.class)
    public void shouldThrowZipExceptionWhileCompressEmptyFolder() throws Exception {
        folderPath = FileUtil.createFolder();
        File folder = new File(folderPath);
        assertTrue(folder.isDirectory());
        FolderCompressor.compress(folderPath);

    }

    @Test
    public void shouldSuccessfullyCompressFolder() throws Exception {
        folderPath = FileUtil.createFolder();
        File folder = new File(folderPath);
        assertTrue(folder.isDirectory());
        InputStream inputStream = IOUtils.toInputStream("some test data for my input stream");
        createFile(inputStream, folderPath + "fileName");
        String zipFolderPath = FolderCompressor.compress(folderPath);
        assertNotNull(zipFolderPath);
        assertFalse(folder.exists());
        FileUtils.forceDelete(new File(zipFolderPath));
    }

    @After
    public void removeTempFolder() throws IOException {
        FileUtils.deleteDirectory(new File(folderPath));
    }


    public File createFile(InputStream inputStream, String fileName) throws IOException {

        File file = new File(fileName);
        OutputStream outStream = new FileOutputStream(file);
        IOUtils.copy(inputStream, outStream);
        outStream.close();
        return file;
    }
}