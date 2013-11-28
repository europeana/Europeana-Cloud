/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.europeana.cloud.service.mcs.persistent;

import org.apache.solr.client.solrj.SolrServer;


public interface SolrConnectionProvider {
    /**
     * Return solr server instance.
     *
     * @return instance of Solr server
     */
    SolrServer getSolrServer();
    
}
