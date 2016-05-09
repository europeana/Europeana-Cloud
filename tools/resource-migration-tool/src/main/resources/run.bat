@echo off
rem set real path from NT variable
set _REALPATH=%~dp0
java -Dlog4j.configuration=file:///%~dp0\log4j.properties -cp lib\aopalliance-1.0.jar;lib\argparse4j-0.7.0.jar;lib\asm-all-repackaged-2.2.0-b21.jar;lib\bean-validator-2.2.0-b21.jar;lib\cal10n-api-0.7.4.jar;lib\cglib-2.2.0-b21.jar;lib\slf4j-api-1.7.12.jar;lib\slf4j-log4j12-1.7.12.jar;lib\class-model-2.2.0-b21.jar;lib\commons-codec-1.9.jar;lib\commons-io-2.4.jar;lib\commons-lang3-3.1.jar;lib\commons-logging-1.2.jar;lib\config-types-2.2.0-b21.jar;lib\core-2.2.0-b21.jar;lib\curator-client-2.4.1.jar;lib\curator-framework-2.4.1.jar;lib\curator-recipes-2.4.1.jar;lib\curator-x-discovery-2.4.1.jar;lib\ecloud-common-0.5-SNAPSHOT.jar;lib\ecloud-service-cos-0.5-SNAPSHOT.jar;lib\ecloud-service-mcs-api-0.5-SNAPSHOT.jar;lib\ecloud-service-mcs-rest-client-java-0.5-SNAPSHOT.jar;lib\ecloud-service-uis-api-0.5-SNAPSHOT.jar;lib\ecloud-service-uis-rest-client-java-0.5-SNAPSHOT.jar;lib\guava-14.0.1.jar;lib\hamcrest-core-1.3.jar;lib\hk2-2.2.0-b21.jar;lib\hk2-api-2.2.0-b21.jar;lib\hk2-config-2.2.0-b21.jar;lib\hk2-locator-2.2.0-b21.jar;lib\hk2-runlevel-2.2.0-b21.jar;lib\hk2-utils-2.2.0-b21.jar;lib\jackson-core-asl-1.9.13.jar;lib\jackson-mapper-asl-1.9.13.jar;lib\javax.annotation-api-1.2.jar;lib\javax.inject-2.2.0-b21.jar;lib\javax.ws.rs-api-2.0.jar;lib\jersey-client-2.4.jar;lib\jersey-common-2.4.jar;lib\jersey-container-servlet-core-2.4.jar;lib\jersey-entity-filtering-2.4.jar;lib\jersey-media-moxy-2.4.jar;lib\jersey-media-multipart-2.4.jar;lib\jersey-server-2.4.jar;lib\jersey-spring3-2.4.jar;lib\jline-0.9.94.jar;lib\junit-4.11.jar;lib\log4j-1.2.17.jar;lib\mimepull-1.9.3.jar;lib\org.eclipse.persistence.antlr-2.5.0.jar;lib\org.eclipse.persistence.asm-2.5.0.jar;lib\org.eclipse.persistence.core-2.5.0.jar;lib\org.eclipse.persistence.moxy-2.5.0.jar;lib\osgi-resource-locator-1.0.1.jar;lib\snakeyaml-1.16.jar;lib\spring-aop-4.2.3.RELEASE.jar;lib\spring-beans-4.2.3.RELEASE.jar;lib\spring-boot-1.3.0.RELEASE.jar;lib\spring-boot-autoconfigure-1.3.0.RELEASE.jar;lib\spring-boot-starter-1.3.0.RELEASE.jar;lib\spring-boot-starter-logging-1.3.0.RELEASE.jar;lib\spring-bridge-2.2.0-b21.jar;lib\spring-context-4.2.3.RELEASE.jar;lib\spring-core-4.2.3.RELEASE.jar;lib\spring-expression-4.2.3.RELEASE.jar;lib\spring-test-4.2.3.RELEASE.jar;lib\spring-web-3.2.3.RELEASE.jar;lib\tiger-types-1.4.jar;lib\validation-api-1.1.0.Final.jar;lib\xbean-spring-2.8.jar;lib\zookeeper-3.4.5.jar;lib\resource-migrator.jar eu.europeana.cloud.migrator.ResourceMigratorApp -path=%_REALPATH% %1 %2