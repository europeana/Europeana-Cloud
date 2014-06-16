/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.europeana.cloud.service.mcs.messages;

/**
 *
 *
 */
public abstract class AbstractMessage {

    String payload;

    public AbstractMessage(String payload) {
	this.payload = payload;

    }
}
