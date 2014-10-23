/**
 * Copyright 2014 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-connectors (FI-WARE project).
 *
 * fiware-connectors is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-connectors is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-connectors. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with Francisco Romero
 * francisco.romerobueno@telefonica.com
 */

package es.tid.fiware.fiwareconnectors.cygnus.http;

import java.util.concurrent.TimeUnit;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.log4j.Logger;

/**
 * Idle connections monitor class, according to this link.
 * http://hc.apache.org/httpcomponents-client-4.3.x/tutorial/html/connmgmt.html#d5e405
 * 
 * @author frb
 */
public class IdleConnectionsMonitor extends Thread {

    private final PoolingClientConnectionManager connectionsManager;
    private int frequency;
    private int idleTime;
    private volatile boolean shutdown;
    private Logger logger;
    
    /**
     * Constructor.
     * @param connectionsManager Connections manager to be monitored
     * @param frequency Monitoring frequency (seconds)
     * @param idleTime Maximum idle time allowed for connections (seconds)
     */
    public IdleConnectionsMonitor(PoolingClientConnectionManager connectionsManager, int frequency, int idleTime) {
        super();
        this.connectionsManager = connectionsManager;
        this.frequency = frequency * 1000;
        this.logger = Logger.getLogger(IdleConnectionsMonitor.class);
    } // IdleConnectionsMonitor

    @Override
    public void run() {
        try {
            while (!shutdown) {
                synchronized (this) {
                    wait(frequency);
                    // close expired connections
                    connectionsManager.closeExpiredConnections();
                    // optionally, close connections that have been idle longer than 30 sec
                    connectionsManager.closeIdleConnections(idleTime, TimeUnit.SECONDS);
                } // synchronized
            } // while
        } catch (InterruptedException e) {
            logger.error("The idle connections monitor has been shutdown. Details= " + e.getMessage());
        } // try catch
    } // run
    
    /**
     * Shutdowns the monitor from an external source.
     */
    public void shutdown() {
        shutdown = true;
        
        synchronized (this) {
            notifyAll();
        } // synchronized
    } // shutdown
    
} // IdleConnectionsMonitor
