/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Recorder;
import org.junit.Rule;

/**
 * 
 * @author krystian
 */
public class FileServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    private final String baseUrl = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/";


    public FileServiceClientTest() {
    }

}
