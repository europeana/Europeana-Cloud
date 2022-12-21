package migrator.validators;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import java.util.HashMap;
import java.util.Map;
import migrator.EmbeddedCassandra;
import org.junit.Assert;

public class V2_validator {

  private Session session;

  public V2_validator(Session session) {
    this.session = session;
  }

  public void validate() {
    KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(EmbeddedCassandra.KEYSPACE);
    TableMetadata basic_info = meta.getTable("basic_info");
    TableMetadata error_notifications = meta.getTable("error_notifications");
    TableMetadata error_counters = meta.getTable("error_counters");

    //
    Assert.assertNotNull(basic_info);
    Assert.assertNotNull(basic_info.getColumn("errors"));

    Assert.assertEquals(basic_info.getColumn("errors").getType(), DataType.cint());

    //
    Assert.assertNotNull(error_notifications);
    Assert.assertNotNull(error_notifications.getColumn("task_id"));
    Assert.assertNotNull(error_notifications.getColumn("error_type"));
    Assert.assertNotNull(error_notifications.getColumn("error_message"));
    Assert.assertNotNull(error_notifications.getColumn("resource"));

    Assert.assertEquals(error_notifications.getColumn("task_id").getType(), DataType.bigint());
    Assert.assertEquals(error_notifications.getColumn("error_type").getType(), DataType.timeuuid());
    Assert.assertEquals(error_notifications.getColumn("error_message").getType(), DataType.text());
    Assert.assertEquals(error_notifications.getColumn("resource").getType(), DataType.varchar());

    //
    Assert.assertNotNull(error_counters);
    Assert.assertNotNull(error_counters.getColumn("task_id"));
    Assert.assertNotNull(error_counters.getColumn("error_type"));
    Assert.assertNotNull(error_counters.getColumn("error_count"));

    Assert.assertEquals(error_counters.getColumn("task_id").getType(), DataType.bigint());
    Assert.assertEquals(error_counters.getColumn("error_type").getType(), DataType.timeuuid());
    Assert.assertEquals(error_counters.getColumn("error_count").getType(), DataType.counter());

    //
    ResultSet rs = session.execute("select * from basic_info");
    Assert.assertTrue(rs.getAvailableWithoutFetching() == 1);
    while (rs.iterator().hasNext()) {
      Row r = rs.iterator().next();
      Assert.assertTrue(r.getLong("task_id") == 1);
      Assert.assertTrue(r.getInt("errors") == 3);
    }

    //
    Map<String, String> errorTypes = new HashMap<>();
    Map<String, Integer> counts = new HashMap<>();

    rs = session.execute("select * from error_notifications");
    Assert.assertTrue(rs.getAvailableWithoutFetching() == 3);
    while (rs.iterator().hasNext()) {
      Row r = rs.iterator().next();
      Assert.assertTrue(r.getLong("task_id") == 1);

      String error_message = r.getString("error_message");
      Assert.assertTrue(error_message.equals("javax.ws.rs.InternalServerErrorException: HTTP 500 Internal Server Error\n" +
          "\tat org.glassfish.jersey.jaxb.internal.AbstractRootElementJaxbProvider.readFrom(AbstractRootElementJaxbProvider.java:138)\n"
          +
          "\tat org.glassfish.jersey.message.internal.ReaderInterceptorExecutor$TerminalReaderInterceptor.invokeReadFrom(ReaderInterceptorExecutor.java:256)\n"
          +
          "\tat org.glassfish.jersey.message.internal.ReaderInterceptorExecutor$TerminalReaderInterceptor.aroundReadFrom(ReaderInterceptorExecutor.java:235)\n"
          +
          "\tat org.glassfish.jersey.message.internal.ReaderInterceptorExecutor.proceed(ReaderInterceptorExecutor.java:155)\n" +
          "\tat org.glassfish.jersey.message.internal.MessageBodyFactory.readFrom(MessageBodyFactory.java:1085)\n" +
          "\tat org.glassfish.jersey.message.internal.InboundMessageContext.readEntity(InboundMessageContext.java:874)\n" +
          "\tat org.glassfish.jersey.message.internal.InboundMessageContext.readEntity(InboundMessageContext.java:808)\n" +
          "\tat org.glassfish.jersey.client.ClientResponse.readEntity(ClientResponse.java:326)\n" +
          "\tat org.glassfish.jersey.client.InboundJaxrsResponse$1.call(InboundJaxrsResponse.java:115)\n" +
          "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:315)\n" +
          "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:297)\n" +
          "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:228)\n" +
          "\tat org.glassfish.jersey.process.internal.RequestScope.runInScope(RequestScope.java:419)\n" +
          "\tat org.glassfish.jersey.client.InboundJaxrsResponse.runInScopeIfPossible(InboundJaxrsResponse.java:267)\n" +
          "\tat org.glassfish.jersey.client.InboundJaxrsResponse.readEntity(InboundJaxrsResponse.java:112)\n" +
          "\tat eu.europeana.cloud.mcs.driver.DataSetServiceClient.getDataSetRepresentationsChunk(DataSetServiceClient.java:284)\n"
          +
          "\tat eu.europeana.cloud.mcs.driver.RepresentationIterator.obtainNextChunk(RepresentationIterator.java:117)\n" +
          "\tat eu.europeana.cloud.mcs.driver.RepresentationIterator.hasNext(RepresentationIterator.java:73)\n" +
          "\tat eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt.emitSingleRepresentationFromDataSet(ReadDatasetBolt.java:83)\n"
          +
          "\tat eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt.execute(ReadDatasetBolt.java:60)\n" +
          "\tat eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.execute(AbstractDpsBolt.java:67)\n" +
          "\tat org.apache.storm.daemon.executor$fn__8058$tuple_action_fn__8060.invoke(executor.clj:731)\n" +
          "\tat org.apache.storm.daemon.executor$mk_task_receiver$fn__7979.invoke(executor.clj:464)\n" +
          "\tat org.apache.storm.disruptor$clojure_handler$reify__7492.onEvent(disruptor.clj:40)\n" +
          "\tat org.apache.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:451)\n" +
          "\tat org.apache.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:430)\n" +
          "\tat org.apache.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:73)\n" +
          "\tat org.apache.storm.daemon.executor$fn__8058$fn__8071$fn__8124.invoke(executor.clj:850)\n" +
          "\tat org.apache.storm.util$async_loop$fn__624.invoke(util.clj:484)\n" +
          "\tat clojure.lang.AFn.run(AFn.java:22)\n" +
          "\tat java.lang.Thread.run(Thread.java:745)\n" +
          "Caused by: com.sun.xml.internal.bind.v2.runtime.IllegalAnnotationsException: 1 counts of IllegalAnnotationExceptions\n"
          +
          "Class has two properties of the same name \"creationTimeStamp\"\n" +
          "\tthis problem is related to the following location:\n" +
          "\t\tat public java.util.Date eu.europeana.cloud.common.model.Revision.getCreationTimeStamp()\n" +
          "\t\tat eu.europeana.cloud.common.model.Revision\n" +
          "\t\tat public java.util.List eu.europeana.cloud.common.model.Representation.getRevisions()\n" +
          "\t\tat eu.europeana.cloud.common.model.Representation\n" +
          "\t\tat @javax.xml.bind.annotation.XmlSeeAlso(value=[class eu.europeana.cloud.common.model.DataProvider, class eu.europeana.cloud.common.model.Representation, class eu.europeana.cloud.common.model.DataSet, class eu.europeana.cloud.common.model.CloudId, class eu.europeana.cloud.common.model.LocalId, class java.lang.String, class eu.europeana.cloud.common.response.CloudVersionRevisionResponse, class eu.europeana.cloud.common.model.CloudIdAndTimestampResponse, class eu.europeana.cloud.common.response.CloudTagsResponse])\n"
          +
          "\tthis problem is related to the following location:\n" +
          "\t\tat private java.util.Date eu.europeana.cloud.common.model.Revision.creationTimeStamp\n" +
          "\t\tat eu.europeana.cloud.common.model.Revision\n" +
          "\t\tat public java.util.List eu.europeana.cloud.common.model.Representation.getRevisions()\n" +
          "\t\tat eu.europeana.cloud.common.model.Representation\n" +
          "\t\tat @javax.xml.bind.annotation.XmlSeeAlso(value=[class eu.europeana.cloud.common.model.DataProvider, class eu.europeana.cloud.common.model.Representation, class eu.europeana.cloud.common.model.DataSet, class eu.europeana.cloud.common.model.CloudId, class eu.europeana.cloud.common.model.LocalId, class java.lang.String, class eu.europeana.cloud.common.response.CloudVersionRevisionResponse, class eu.europeana.cloud.common.model.CloudIdAndTimestampResponse, class eu.europeana.cloud.common.response.CloudTagsResponse])\n"
          +
          "\n" +
          "\tat com.sun.xml.internal.bind.v2.runtime.IllegalAnnotationsException$Builder.check(IllegalAnnotationsException.java:91)\n"
          +
          "\tat com.sun.xml.internal.bind.v2.runtime.JAXBContextImpl.getTypeInfoSet(JAXBContextImpl.java:445)\n" +
          "\tat com.sun.xml.internal.bind.v2.runtime.JAXBContextImpl.<init>(JAXBContextImpl.java:277)\n" +
          "\tat com.sun.xml.internal.bind.v2.runtime.JAXBContextImpl.<init>(JAXBContextImpl.java:124)\n" +
          "\tat com.sun.xml.internal.bind.v2.runtime.JAXBContextImpl$JAXBContextBuilder.build(JAXBContextImpl.java:1123)\n" +
          "\tat com.sun.xml.internal.bind.v2.ContextFactory.createContext(ContextFactory.java:147)\n" +
          "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
          "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n" +
          "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n" +
          "\tat java.lang.reflect.Method.invoke(Method.java:498)\n" +
          "\tat javax.xml.bind.ContextFinder.newInstance(ContextFinder.java:247)\n" +
          "\tat javax.xml.bind.ContextFinder.newInstance(ContextFinder.java:234)\n" +
          "\tat javax.xml.bind.ContextFinder.find(ContextFinder.java:462)\n" +
          "\tat javax.xml.bind.JAXBContext.newInstance(JAXBContext.java:641)\n" +
          "\tat javax.xml.bind.JAXBContext.newInstance(JAXBContext.java:584)\n" +
          "\tat org.glassfish.jersey.jaxb.internal.AbstractJaxbProvider.getStoredJaxbContext(AbstractJaxbProvider.java:311)\n" +
          "\tat org.glassfish.jersey.jaxb.internal.AbstractJaxbProvider.getJAXBContext(AbstractJaxbProvider.java:296)\n" +
          "\tat org.glassfish.jersey.jaxb.internal.AbstractJaxbProvider.getUnmarshaller(AbstractJaxbProvider.java:212)\n" +
          "\tat org.glassfish.jersey.jaxb.internal.AbstractJaxbProvider.getUnmarshaller(AbstractJaxbProvider.java:187)\n" +
          "\tat org.glassfish.jersey.jaxb.internal.AbstractRootElementJaxbProvider.readFrom(AbstractRootElementJaxbProvider.java:134)\n"
          +
          "\t... 30 more\n") || error_message.equals(
          "java.lang.IllegalStateException: The template variable 'F80A579D-DBA3-5AE6-E044-001A4B08D326' has no value\n" +
              "\tat org.glassfish.jersey.client.JerseyWebTarget.getUri(JerseyWebTarget.java:134)\n" +
              "\tat org.glassfish.jersey.client.JerseyWebTarget.request(JerseyWebTarget.java:214)\n" +
              "\tat org.glassfish.jersey.client.JerseyWebTarget.request(JerseyWebTarget.java:59)\n" +
              "\tat eu.europeana.cloud.client.uis.rest.UISClient.getCloudId(UISClient.java:143)\n" +
              "\tat eu.europeana.cloud.service.dps.storm.io.OAIWriteRecordBolt.getCloudId(OAIWriteRecordBolt.java:48)\n" +
              "\tat eu.europeana.cloud.service.dps.storm.io.OAIWriteRecordBolt.createRepresentationAndUploadFile(OAIWriteRecordBolt.java:38)\n"
              +
              "\tat eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt.uploadFileInNewRepresentation(WriteRecordBolt.java:63)\n"
              +
              "\tat eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt.execute(WriteRecordBolt.java:44)\n" +
              "\tat eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.execute(AbstractDpsBolt.java:67)\n" +
              "\tat org.apache.storm.daemon.executor$fn__8058$tuple_action_fn__8060.invoke(executor.clj:731)\n" +
              "\tat org.apache.storm.daemon.executor$mk_task_receiver$fn__7979.invoke(executor.clj:464)\n" +
              "\tat org.apache.storm.disruptor$clojure_handler$reify__7492.onEvent(disruptor.clj:40)\n" +
              "\tat org.apache.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:451)\n" +
              "\tat org.apache.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:430)\n" +
              "\tat org.apache.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:73)\n" +
              "\tat org.apache.storm.daemon.executor$fn__8058$fn__8071$fn__8124.invoke(executor.clj:850)\n" +
              "\tat org.apache.storm.util$async_loop$fn__624.invoke(util.clj:484)\n" +
              "\tat clojure.lang.AFn.run(AFn.java:22)\n" +
              "\tat java.lang.Thread.run(Thread.java:745)\n" +
              "Caused by: java.lang.IllegalArgumentException: The template variable 'F80A579D-DBA3-5AE6-E044-001A4B08D326' has no value\n"
              +
              "\tat org.glassfish.jersey.uri.UriTemplate$1ValuesFromArrayStrategy.valueFor(UriTemplate.java:1020)\n" +
              "\tat org.glassfish.jersey.uri.UriTemplate.resolveTemplate(UriTemplate.java:706)\n" +
              "\tat org.glassfish.jersey.uri.UriTemplate.createUriComponent(UriTemplate.java:1030)\n" +
              "\tat org.glassfish.jersey.uri.UriTemplate.createURIWithStringValues(UriTemplate.java:970)\n" +
              "\tat org.glassfish.jersey.uri.UriTemplate.createURIWithStringValues(UriTemplate.java:906)\n" +
              "\tat org.glassfish.jersey.uri.UriTemplate.createURI(UriTemplate.java:871)\n" +
              "\tat org.glassfish.jersey.uri.internal.JerseyUriBuilder._build(JerseyUriBuilder.java:914)\n" +
              "\tat org.glassfish.jersey.uri.internal.JerseyUriBuilder.build(JerseyUriBuilder.java:831)\n" +
              "\tat org.glassfish.jersey.client.JerseyWebTarget.getUri(JerseyWebTarget.java:132)\n" +
              "\t... 18 more\n"));

      String error_type = r.getUUID("error_type").toString();
      if (errorTypes.get(error_message) == null) {
        errorTypes.put(error_message, error_type);
      }
      Assert.assertTrue(error_type.equals(errorTypes.get(error_message)));
      String resource = r.getString("resource");
      Assert.assertTrue(
          "http://195.216.97.81:8080/mcs/records/2WYNVPQMZHEO6HVDGIVTR4IKEB54FHH2X274YDW6NXSF25KI75NA/representations/IC_REPRESENTATION/versions/5eaea4d0-ceac-11e7-b72d-fa163e1e1541/files/0a8573e1-13a0-4431-bfb2-ce73fded7e5d".equals(
              resource)
              || "http://195.216.97.81:8080/mcs/records/2WYNVPQMZHEO6HVDGIVTR4IKEB54FHH2X274YDW6NXSF25KI75NA/representations/IC_REPRESENTATION/versions/5eaea4d0-ceac-11e7-b72d-fa163e1e1541/files/5133488d-03d9-4bb5-851c-67bfdbcd8542".equals(
              resource)
              || "oai:lib.psnc.pl:614".equals(resource));
      if (counts.get(error_type) == null) {
        counts.put(error_type, 1);
      } else {
        counts.put(error_type, counts.get(error_type) + 1);
      }
    }

    //
    rs = session.execute("select * from error_counters");
    Assert.assertTrue(rs.getAvailableWithoutFetching() == 2);
    while (rs.iterator().hasNext()) {
      Row r = rs.iterator().next();
      Assert.assertTrue(r.getLong("task_id") == 1);
      String error_type = r.getUUID("error_type").toString();
      Assert.assertTrue(errorTypes.containsValue(error_type));
      Assert.assertTrue(counts.get(error_type) == r.getLong("error_count"));
    }
  }
}
