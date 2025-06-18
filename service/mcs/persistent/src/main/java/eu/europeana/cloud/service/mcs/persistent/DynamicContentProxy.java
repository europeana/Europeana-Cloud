package eu.europeana.cloud.service.mcs.persistent;

import static org.apache.commons.lang.Validate.notEmpty;

import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.ContentDaoNotFoundException;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.PutResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.Map;


/**
 * Proxy that switch contentDAO based on {@link Storage} value.
 *
 * @author krystian.
 */
public class DynamicContentProxy {

  private final Map<Storage, ContentDAO> contentDAOs = new EnumMap<>(Storage.class);

  public DynamicContentProxy(Map<Storage, ContentDAO> contentDAOs) {
    notEmpty(contentDAOs);
    this.contentDAOs.putAll(contentDAOs);
  }

  public void deleteContent(String md5, String fileName, Storage stored) throws FileNotExistsException {
    if (Storage.DATA_BASE.equals(stored)) {
      getContentDAO(Storage.DB_STORAGE).deleteContent(md5, fileName);
    } else {
      getContentDAO(stored).deleteContent(md5, fileName);
    }
  }

  public void getContent(String md5, String fileName, long start, long end, OutputStream os, Storage stored)
      throws IOException, FileNotExistsException {

    if (Storage.DATA_BASE.equals(stored)) {
      getContentDAO(Storage.DB_STORAGE).getContent(md5, fileName, start, end, os);;
    } else {
      getContentDAO(stored).getContent(md5, fileName, start, end, os);
    }
  }

  public PutResult putContent(String fileName, InputStream data, Storage stored) throws IOException {

    if (Storage.DATA_BASE.equals(stored)) {
      return getContentDAO(Storage.DB_STORAGE).putContent(fileName, data);
    } else {
      return getContentDAO(stored).putContent(fileName, data);
    }
  }

  private ContentDAO getContentDAO(final Storage storage) {
    ContentDAO dao;
    if (Storage.DATA_BASE.equals(storage)) {
      dao = contentDAOs.get(Storage.DB_STORAGE);
    } else {
      dao = contentDAOs.get(storage);
    }
    if (dao == null) {
      throw new ContentDaoNotFoundException("Specified storage \"" + storage + "\" has not been defined in " + this + "!");
    }
    return dao;
  }
}
