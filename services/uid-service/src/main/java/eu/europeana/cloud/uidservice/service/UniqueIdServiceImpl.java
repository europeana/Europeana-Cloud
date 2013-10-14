package eu.europeana.cloud.uidservice.service;

import org.springframework.stereotype.Service;

@Service
public class UniqueIdServiceImpl implements UniqueIdService {
	
	@Override
	public  String create(String providerId,String recordId){
		return String.format("/%s/%s", providerId,recordId);
	}
}
