package eu.europeana.cloud.http.service;

import eu.europeana.cloud.http.common.CompressionFileExtension;
import eu.europeana.cloud.http.common.UnpackingServiceFactory;
import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import org.apache.commons.io.FilenameUtils;
import org.zeroturnaround.zip.NameMapper;
import org.zeroturnaround.zip.ZipUtil;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ZipUnpackingService implements FileUnpackingService {
    public void unpackFile(final String compressedFilePath, final String destinationFolder) throws CompressionExtensionNotRecognizedException, IOException {
        final List<String> zipFiles = new ArrayList<>();
        ZipUtil.unpack(new File(compressedFilePath), new File(destinationFolder), new NameMapper() {
            public String map(String name) {
                if (CompressionFileExtension.contains(FilenameUtils.getExtension(name))) {
                    String compressedFilePath = destinationFolder + name;
                    zipFiles.add(compressedFilePath);
                }
                return name;
            }
        });
        for (String nestedCompressedFile : zipFiles) {
            String extension = FilenameUtils.getExtension(nestedCompressedFile);
            UnpackingServiceFactory.createUnpackingService(extension).unpackFile(
                    nestedCompressedFile,
                    FilenameUtils.removeExtension(nestedCompressedFile) + File.separator
            );
        }
    }
}
