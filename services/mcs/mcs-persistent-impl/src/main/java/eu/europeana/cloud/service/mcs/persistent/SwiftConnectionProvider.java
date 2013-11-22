package eu.europeana.cloud.service.mcs.persistent;

import java.util.Properties;
import javax.annotation.PreDestroy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SwiftConnectionProvider
{

	private final static Logger log = LoggerFactory.getLogger(SwiftConnectionProvider.class);
	private final BlobStoreContext context;
	private final String container;
	private final BlobStore blobStore;


	public SwiftConnectionProvider(String provider, String container, String endpoint, String user, String password)
	{
		this.container = container;
		Properties properties = new Properties();
		properties.setProperty("swift.endpoint", endpoint);
		context = ContextBuilder.newBuilder(provider) //put "swift" in config
				.overrides(properties).credentials(user, password).buildView(BlobStoreContext.class);
		blobStore = context.getBlobStore();
		if (!blobStore.containerExists(container)) {
			blobStore.createContainerInLocation(null, container);
		}
		log.info("Connected to swift");
	}


	@PreDestroy
	public void closeConnections()
	{
		log.info("Shutting down swift connection");
	}


	public BlobStoreContext getContext()
	{
		return context;
	}


	public String getContainer()
	{
		return container;
	}


	public BlobStore getBlobStore()
	{
		return blobStore;
	}

}
