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
package com.telefonica.iot.cygnus.backends.orion;

import java.util.ArrayList;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHttpResponse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author frb
 */
@RunWith(MockitoJUnitRunner.class)
public class OrionBackendImplTest {
    
    // instance to be tested
    private OrionBackendImpl backend;
    
    // mocks
    // the DefaultHttpClient class cannot be mocked:
    // http://stackoverflow.com/questions/4547852/why-does-my-mockito-mock-object-use-real-the-implementation
    @Mock
    private HttpClient mockHttpClient;
    
    // constants
    private final String orionHost = "localhost";
    private final String orionPort = "1026";
    private final String entityId = "car1";
    private final String entityType = "car";
    private final String attrName = "speed";
    private final String attrType = "float";
    private final BasicHttpResponse resp200 = new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), 200, "OK");
    private final ArrayList<OrionStats> nullAttrStats = null;
    private final ArrayList<OrionStats> emptyAttrStats = new ArrayList<OrionStats>();
    private final ArrayList<OrionStats> attrStats = new ArrayList<OrionStats>();
    private final String metadata = ""
            + "["
            + "   {"
            + "      \"name\": \"orion_stats_sink_prev_value\","
            + "      \"type\": \"double\","
            + "      \"value\": 112.9"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_prev_ts\","
            + "      \"type\": \"double\","
            + "      \"value\": 123533563"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_num_values\","
            + "      \"type\": \"double\","
            + "      \"value\": 3048"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_max_value\","
            + "      \"type\": \"double\","
            + "      \"value\": 138.9"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_min_value\","
            + "      \"type\": \"double\","
            + "      \"value\": 2.3"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_sum_values\","
            + "      \"type\": \"double\","
            + "      \"value\": 421001.0"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_sum2_values\","
            + "      \"type\": \"double\","
            + "      \"value\": 453111340.0"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_max_diff_values\","
            + "      \"type\": \"double\","
            + "      \"value\": 12.1"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_min_diff_values\","
            + "      \"type\": \"double\","
            + "      \"value\": 0.2"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_max_diff_ts\","
            + "      \"type\": \"double\","
            + "      \"value\": 1001.0"
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_min_diff_ts\","
            + "      \"type\": \"double\","
            + "      \"value\": 989.0"
            + "   },"
/*
            + "   {"
            + "      \"name\": \"orion_stats_sink_clusters\","
            + "      \"type\": \"double\","
            + "      \"value\": \"\""
            + "   },";
*/
            + "   {"
            + "      \"name\": \"orion_stats_sink_values_to_monitor\","
            + "      \"type\": \"double\","
            + "      \"value\": ["
            + "         {\"mon_value\": 120.0,\"num_occur\": 307},"
            + "         {\"mon_value\": 60.0,\"num_occur\": 141}"
            + "      ]"
            + "   }"
            + "]";
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        backend = new OrionBackendImpl(orionHost, orionPort);
        
        // set up other instances
        OrionStats stats = new OrionStats(attrName, attrType, metadata, true, true, true, null);
        attrStats.add(stats);
        
        // set up the behaviour of the mocked classes
        when(mockHttpClient.execute(Mockito.any(HttpUriRequest.class))).thenReturn(resp200);
    } // setUp
    
    /**
     * Test of updateContext method, of class OrionBackendImpl. Null stats are passed.
     */
    @Test
    public void testUpdateContextNullStats() {
        System.out.println("Testing OrionBackendImpl.updateContext (null stats)");
        
        try {
            backend.setHttpClient(mockHttpClient);
            backend.updateContext(entityId, entityType, nullAttrStats);
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testUpdateContextNullStats
    
    /**
     * Test of updateContext method, of class OrionBackendImpl. Empty stats are passed.
     */
    @Test
    public void testUpdateContextEmptyStats() {
        System.out.println("Testing OrionBackendImpl.updateContext (empty stats)");
        
        try {
            backend.setHttpClient(mockHttpClient);
            backend.updateContext(entityId, entityType, emptyAttrStats);
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testUpdateContextEmptyStats
    
    /**
     * Test of updateContext method, of class OrionBackendImpl.
     */
    @Test
    public void testUpdateContext() {
        System.out.println("Testing OrionBackendImpl.updateContext");
        
        try {
            backend.setHttpClient(mockHttpClient);
            backend.updateContext(entityId, entityType, attrStats);
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testUpdateContext
    
} // OrionBackendImplTest
