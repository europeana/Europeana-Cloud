package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import java.util.Iterator;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;


public class UISClientHandler {
    
    @Autowired
    private UISClient uisClient;
    
     public boolean recordExistInUIS(String cloudId) throws RecordNotExistsException  {
        boolean result = false;
        try {
            List<CloudId> records = uisClient.getRecordId(cloudId);
            if(records==null) {
                throw new RecordNotExistsException(cloudId);
            }
            else {
                if(!records.isEmpty()){
                    Iterator<CloudId> iterator = records.iterator();
                    while(iterator.hasNext()){
                        CloudId ci = iterator.next();
                        if(ci.getId().equals(cloudId)){
                            result = true;
                            break;
                        }
                    }
                    if(result==false)
                        throw new RecordNotExistsException(cloudId);
                } else throw new RecordNotExistsException(cloudId);
            }
        } catch (CloudException ex) {
            if(ex.getCause() instanceof RecordDoesNotExistException) {
                throw new RecordNotExistsException(cloudId);
            } else throw new SystemException(ex);
        }
        return result;
    }
}
