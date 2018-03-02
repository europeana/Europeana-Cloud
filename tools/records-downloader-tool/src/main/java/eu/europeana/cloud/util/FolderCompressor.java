package eu.europeana.cloud.util;

import org.zeroturnaround.zip.ZipException;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;

/**
 * Created by Tarek on 9/1/2016.
 */
public class FolderCompressor {
    public static void compress(String folderPath, String zipFolderPath) throws ZipException {
        File folder = new File(folderPath);
        ZipUtil.pack(folder, new File(zipFolderPath));
    }
}
