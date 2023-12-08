package eu.europeana.cloud.service.uis.service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import java.util.List;

import eu.europeana.metis.utils.CommonStringValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data provider service using Cassandra as database.
 */
public class CassandraDataProviderService implements DataProviderService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataProviderService.class);
  private final CassandraDataProviderDAO dataProviderDao;

  public CassandraDataProviderService(CassandraDataProviderDAO dataProviderDao) {
    this.dataProviderDao = dataProviderDao;
  }


  @Override
  public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("getProviders() thresholdProviderId='{}', limit='{}'",
              CommonStringValues.CRLF_PATTERN.matcher(thresholdProviderId).replaceAll(""), limit);
    }
    String nextProvider = null;
    List<DataProvider> providers = dataProviderDao.getProviders(thresholdProviderId, limit + 1);
    final int providerSize = providers.size();
    if (providerSize == limit + 1) {
      nextProvider = providers.get(limit).getId();
      providers.remove(limit);
    }
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("getProviders() returning providers={} and nextProvider={} for thresholdProviderId='{}', limit='{}'",
              providerSize,
              CommonStringValues.CRLF_PATTERN.matcher(nextProvider).replaceAll(""),
              CommonStringValues.CRLF_PATTERN.matcher(thresholdProviderId).replaceAll(""),
              limit);
    }
    return new ResultSlice<>(nextProvider, providers);
  }


  @Override
  public DataProvider getProvider(String providerId)
      throws ProviderDoesNotExistException {
    LOGGER.info("getProvider() providerId='{}'", providerId);
    DataProvider dp = dataProviderDao.getProvider(providerId);
    if (dp == null) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.warn("ProviderDoesNotExistException providerId='{}''",
                CommonStringValues.CRLF_PATTERN.matcher(providerId).replaceAll(""));
      }
      throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
    } else {
      return dp;
    }
  }


  @Override
  public DataProvider createProvider(String providerId, DataProviderProperties properties)
      throws ProviderAlreadyExistsException {
    if(LOGGER.isInfoEnabled()){
      LOGGER.info("createProvider() providerId='{}', properties='{}'",
              CommonStringValues.CRLF_PATTERN.matcher(providerId).replaceAll(""),
              properties);
    }
    DataProvider dp = dataProviderDao.getProvider(providerId);
    if (dp != null) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.warn("ProviderAlreadyExistsException providerId='{}', properties='{}'",
                CommonStringValues.CRLF_PATTERN.matcher(providerId).replaceAll(""),
                properties);
      }
      throw new ProviderAlreadyExistsException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getHttpCode(),
          IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getErrorInfo(providerId)));
    }
    return dataProviderDao.createDataProvider(providerId, properties);
  }


  @Override
  public DataProvider updateProvider(String providerId, DataProviderProperties properties)
      throws ProviderDoesNotExistException {
    LOGGER.info("updateProvider() providerId='{}', properties='{}'", providerId, properties);
    DataProvider dp = dataProviderDao.getProvider(providerId);
    if (dp == null) {
      LOGGER.warn("ProviderDoesNotExistException providerId='{}', properties='{}'", providerId, properties);
      throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
    } else {
      dp.setProperties(properties);
      return dataProviderDao.updateDataProvider(dp);
    }
  }

  @Override
  public DataProvider updateProvider(DataProvider dataProvider) throws ProviderDoesNotExistException {
    LOGGER.info("updating data provider providerId='{}', properties='{}'", dataProvider.getId(), dataProvider.getProperties());
    DataProvider dp = dataProviderDao.getProvider(dataProvider.getId());
    if (dp == null) {
      LOGGER.warn("ProviderDoesNotExistException providerId='{}', properties='{}'", dataProvider.getId(),
          dataProvider.getProperties());
      throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(dataProvider.getId())));
    }
    return dataProviderDao.updateDataProvider(dataProvider);
  }
}
