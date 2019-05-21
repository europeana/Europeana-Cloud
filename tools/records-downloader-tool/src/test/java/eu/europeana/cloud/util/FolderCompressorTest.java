package eu.europeana.cloud.util;

import eu.europeana.cloud.TestConstantsHelper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Test;
import org.zeroturnaround.zip.ZipException;

import java.io.*;
import java.util.Date;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FolderCompressorTest implements TestConstantsHelper {
    String folderPath;
    String zipFolderPath;

    @Test(expected = ZipException.class)
    public void shouldThrowZipExceptionWhileCompressEmptyFolder() throws Exception {
        folderPath = FileUtil.createFolder();
        File folder = new File(folderPath);
        assertTrue(folder.isDirectory());
        zipFolderPath = FileUtil.createZipFolderPath(new Date());
        FolderCompressor.compress(folderPath, zipFolderPath);
        System.out.println(folderPath);

    }

    @Test
    public void shouldSuccessfullyCompressFolder() throws Exception {
        folderPath = FileUtil.createFolder();
        File folder = new File(folderPath);
        assertTrue(folder.isDirectory());
        InputStream inputStream = IOUtils.toInputStream("some test data for my input stream");
        createFile(inputStream, folderPath + "fileName");
        zipFolderPath = FileUtil.createZipFolderPath(new Date());
        FolderCompressor.compress(folderPath, zipFolderPath);
        assertNotNull(zipFolderPath);

    }

    @After
    public void removeTempFolder() throws IOException {
        FileUtils.deleteDirectory(new File(folderPath));
        if (zipFolderPath != null)
            FileUtils.forceDelete(new File(zipFolderPath));
    }


    public File createFile(InputStream inputStream, String fileName) throws IOException {

        File file = new File(fileName);
        OutputStream outStream = new FileOutputStream(file);
        IOUtils.copy(inputStream, outStream);
        outStream.close();
        return file;
    }
}