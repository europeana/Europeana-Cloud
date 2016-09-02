package eu.europeana.cloud.util;

import org.apache.commons.io.FileUtils;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.IOException;

import org.zeroturnaround.zip.ZipException;

/**
 * Created by Tarek on 9/1/2016.
 */
public class FolderCompressor {
    final static String ZIP_FORMATE_EXTENSION = ".zip";

    public static String compress(String folderPath) throws ZipException, IOException {
        File folder = new File(folderPath);
        String zipFolderPath = FileUtil.buildPath(folder.getParent(), folder.getName(), ZIP_FORMATE_EXTENSION);
        ZipUtil.pack(folder, new File(zipFolderPath));
        FileUtils.deleteDirectory(new File(folderPath));
        return zipFolderPath;

    }
}
