package eu.europeana.cloud.service.dps.storm.utils;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.serialization.DefaultKryoFactory;
import org.apache.storm.serialization.IKryoFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * Kryo factory useful if you don't want Storm's {@link DefaultKryoFactory}
 * behavior of forcing {@link JavaSerializer} for all classes that don't have a
 * serializer explicitly defined in Strom config.
 */
public class KryoFactory implements IKryoFactory {
	
	@Override
	public Kryo getKryo(Map conf) {
		Kryo k = new Kryo();
		k.setRegistrationRequired(!((Boolean) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));
		k.setReferences(false);
		return k;
	}
	
	@Override
	public void preRegister(Kryo k, Map conf) {
		// nothing to do
	}
	
	@Override
	public void postRegister(Kryo k, Map conf) {
		// nothing to do
	}
	
	@Override
	public void postDecorate(Kryo k, Map conf) {
		// nothing to do
	}
	
}
