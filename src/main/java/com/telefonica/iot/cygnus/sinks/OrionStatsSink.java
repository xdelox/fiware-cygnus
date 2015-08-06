/**
 * Copyright 2015 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FI-WARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */

package com.telefonica.iot.cygnus.sinks;

import com.telefonica.iot.cygnus.backends.orion.OrionBackendImpl;
import com.telefonica.iot.cygnus.backends.orion.OrionStats;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextAttribute;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextElement;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextElementResponse;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.utils.Utils;
import java.util.ArrayList;
import java.util.Map;
import org.apache.flume.Context;

/**
 * OrionStatsSink generates online statistics given a certain number of configured entity's attributes from Orion.
 * Online means no previous measure is stored, but statistics are iteratively built given the previous value of the
 statistics and the current measure.
 
 This class feeds Orion as well, by updating a "_stats" sufixed version of the original entity (see OrionBackendImpl
 implementation).
 * 
 * @author frb
 */
public class OrionStatsSink extends OrionSink {
    
    private static final CygnusLogger LOGGER = new CygnusLogger(OrionStatsSink.class);
    private String orionHost;
    private String orionPort;
    private boolean enableValueAnalysis;
    private boolean enableDiffAnalysis;
    private boolean enableClusteringAnalysis;
    private String valuesToMonitor;
    private OrionBackendImpl backend;
    
    /**
     * Constructor.
     */
    public OrionStatsSink() {
        super();
    } // OrionStatsSink
    
    /**
     * Gets the Orion host. It is protected due to it is only required for testing purposes.
     * @return
     */
    protected String getOrionHost() {
        return orionHost;
    } // getOrionHost
    
    /**
     * Gets the Orion port. It is protected due to it is only required for testing purposes.
     * @return
     */
    protected String getOrionPort() {
        return orionPort;
    } // getOrionPort
    
    /**
     * Gets the persistence backend. It is protected due to it is only required for testing purposes.
     * @return
     */
    protected OrionBackendImpl getPersistenceBackend() {
        return backend;
    } // getPersistenceBackend
    
    /**
     * Sets the persistence backend. It is protected due to it is only required for testing purposes.
     * @param backend
     */
    protected void setPersistenceBackend(OrionBackendImpl backend) {
        this.backend = backend;
    } // setPersistenceBackend
    
    @Override
    public void configure(Context context) {
        orionHost = context.getString("orion_host", "localhost");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (orion_host=" + orionHost + ")");
        orionPort = context.getString("orion_port", "1026");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (orion_port=" + orionPort + ")");
        enableValueAnalysis = context.getBoolean("enable_value_analysis", true);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (enable_value_analysis="
                + (enableValueAnalysis ? "true" : "false") + ")");
        enableDiffAnalysis = context.getBoolean("enable_diff_analysis", false);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (enable_diff_analysis="
                + (enableDiffAnalysis ? "true" : "false") + ")");
        enableClusteringAnalysis = false;
/*
        TBD
        enableClusteringAnalysis = context.getBoolean("enable_clustering_analysis", false);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (enable_clustering_analysis="
                + (enableClusteringAnalysis ? "true" : "false") + ")");
*/
        valuesToMonitor = context.getString("values_to_monitor");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (values_to_monitor=" + valuesToMonitor + ")");
    } // configure

    @Override
    public void start() {
        // create the persistence backend
        backend = new OrionBackendImpl(orionHost, orionPort);
        
        // start
        super.start();
        LOGGER.info("[" + this.getName() + "] Startup completed");
    } // start
    
    @Override
    void persist(Map<String, String> eventHeaders, NotifyContextRequest notification) throws Exception {
        // get some header values
        Long recvTimeTs = new Long(eventHeaders.get("timestamp"));
        
        // iterate on the contextResponses
        ArrayList contextResponses = notification.getContextResponses();
        
        for (Object contextResponse : contextResponses) {
            // get the i-th contextElement
            ContextElementResponse contextElementResponse = (ContextElementResponse) contextResponse;
            ContextElement contextElement = contextElementResponse.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + this.getName() + "] Processing context element (id=" + entityId + ", type=" + entityType
                    + ")");
            
            // iterate on all this CKANBackend's attributes, if there are attributes
            ArrayList<ContextAttribute> contextAttributes = contextElement.getAttributes();
            
            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId + ", type="
                        + entityType + ")");
                continue;
            } // if
            
            ArrayList<OrionStats> allAttrStats = new ArrayList<OrionStats>();
            
            for (ContextAttribute contextAttribute : contextAttributes) {
                String attrValue = contextAttribute.getContextValue(false);
                
                if (!Utils.isANumber(attrValue)) {
                    continue;
                } // if
                
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                String attrMetadata = contextAttribute.getContextMetadata();
                LOGGER.debug("[" + this.getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ", metadata=" + attrMetadata + ")");
                
                // update the statistics
                OrionStats stats = new OrionStats(attrName, attrType, attrMetadata, enableValueAnalysis,
                        enableDiffAnalysis, enableClusteringAnalysis, valuesToMonitor);
                stats.update(new Double(attrValue), recvTimeTs);
            } // for
            
            backend.updateContext(entityId, entityType, allAttrStats);
        } // for
    } // persist

} // OrionStatsSink

