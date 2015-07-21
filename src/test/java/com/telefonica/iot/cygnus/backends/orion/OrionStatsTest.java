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
import java.util.HashMap;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author frb
 */
public class OrionStatsTest {
    
    // instance to be tested
    private OrionStats statsEmptyMetadata;
    private OrionStats statsExistentMetadata;
    
    // constants
    private final String attrName = "speed";
    private final String attrType = "float";
    private final String nullMetadata = null;
    private final String emptyMetadata = "";
    private final double prevValue = 112.9;
    private final long prevTs = 1239342867;
    private final long numValues = 1089;
    private final double maxValue = 138.7;
    private final double minValue = 2.3;
    private final double sumValues = 148008.0;
    private final double sum2Values = 15681600.0;
    private final double maxDiffValues = 2.5;
    private final double minDiffValues = 0.2;
    private final double maxDiffTs = 1001.0;
    private final double minDiffTs = 998.0;
    private final double monValue1 = 60.0;
    private final long numOccur1 = 123;
    private final double monValue2 = 120.0;
    private final long numOccur2 = 703;
    private final String existentMetadata = ""
            + "["
            + "   {"
            + "      \"name\": \"orion_stats_sink_prev_value\","
            + "      \"type\": \"double\","
            + "      \"value\": " + prevValue
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_prev_ts\","
            + "      \"type\": \"double\","
            + "      \"value\": " + prevTs
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_num_values\","
            + "      \"type\": \"double\","
            + "      \"value\": " + numValues
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_max_value\","
            + "      \"type\": \"double\","
            + "      \"value\": " + maxValue
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_min_value\","
            + "      \"type\": \"double\","
            + "      \"value\": " + minValue
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_sum_values\","
            + "      \"type\": \"double\","
            + "      \"value\": " + sumValues
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_sum2_values\","
            + "      \"type\": \"double\","
            + "      \"value\": " + sum2Values
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_max_diff_values\","
            + "      \"type\": \"double\","
            + "      \"value\": " + maxDiffValues
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_min_diff_values\","
            + "      \"type\": \"double\","
            + "      \"value\": " + minDiffValues
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_max_diff_ts\","
            + "      \"type\": \"double\","
            + "      \"value\": " + maxDiffTs
            + "   },"
            + "   {"
            + "      \"name\": \"orion_stats_sink_min_diff_ts\","
            + "      \"type\": \"double\","
            + "      \"value\": " + minDiffTs
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
            + "         {\"mon_value\":" + monValue1 + ",\"num_occur\":" + numOccur1 + "},"
            + "         {\"mon_value\":" + monValue2 + ",\"num_occur\":" + numOccur2 + "}"
            + "      ]"
            + "   }"
            + "]";
    private final String valuesToMonitorStr = "60.0,120.0";
    private final double valueForUpdating = 120;
    private final long tsForUpdating = 1239343667;
    
    /**
     * Test of constructor method, of class OrionHDFSSink. Null metadata is passed.
     */
    @Test
    public void testConstructorNullMetadata() {
        System.out.println("Testing OrionStats.constructor (with null metadata)");
        statsEmptyMetadata = new OrionStats(attrName, attrType, nullMetadata, true, true, true, valuesToMonitorStr);
        assertEquals(attrName, statsEmptyMetadata.getAttrName());
        assertEquals(attrType, statsEmptyMetadata.getAttrType());
        assertEquals(Double.POSITIVE_INFINITY, statsEmptyMetadata.getPrevValue(), 0);
        assertEquals(Long.MAX_VALUE, statsEmptyMetadata.getPrevTs(), 0);
        assertEquals(0, statsEmptyMetadata.getNumValues(), 0);
        assertEquals(Double.MIN_VALUE, statsEmptyMetadata.getMaxValue(), 0);
        assertEquals(Double.MAX_VALUE, statsEmptyMetadata.getMinValue(), 0);
        assertEquals(0, statsEmptyMetadata.getSumValues(), 0);
        assertEquals(0, statsEmptyMetadata.getSum2Values(), 0);
        assertEquals(0, statsEmptyMetadata.getMaxDiffValues(), 0);
        assertEquals(0, statsEmptyMetadata.getMinDiffValues(), 0);
        assertEquals(0, statsEmptyMetadata.getMaxDiffTs(), 0);
        assertEquals(0, statsEmptyMetadata.getMinDiffTs(), 0);
        ArrayList<HashMap<String, Object>> valuesToMonitor = statsEmptyMetadata.getValuesToMonitor();
        assertEquals(2, valuesToMonitor.size());
        assertEquals(monValue1, (Double) valuesToMonitor.get(0).get("mon_value"), 0);
        assertEquals(0, (Long) valuesToMonitor.get(0).get("num_occur"), 0);
        assertEquals(monValue2, (Double) valuesToMonitor.get(1).get("mon_value"), 0);
        assertEquals(0, (Long) valuesToMonitor.get(1).get("num_occur"), 0);
    } // testConstructorNullMetadata
    
    /**
     * Test of constructor method, of class OrionHDFSSink. Empty metadata is passed.
     */
    @Test
    public void testConstructorEmptyMetadata() {
        System.out.println("Testing OrionStats.constructor (with empty metadata)");
        statsEmptyMetadata = new OrionStats(attrName, attrType, emptyMetadata, true, true, true, valuesToMonitorStr);
        assertEquals(attrName, statsEmptyMetadata.getAttrName());
        assertEquals(attrType, statsEmptyMetadata.getAttrType());
        assertEquals(Double.POSITIVE_INFINITY, statsEmptyMetadata.getPrevValue(), 0);
        assertEquals(Long.MAX_VALUE, statsEmptyMetadata.getPrevTs(), 0);
        assertEquals(0, statsEmptyMetadata.getNumValues(), 0);
        assertEquals(Double.MIN_VALUE, statsEmptyMetadata.getMaxValue(), 0);
        assertEquals(Double.MAX_VALUE, statsEmptyMetadata.getMinValue(), 0);
        assertEquals(0, statsEmptyMetadata.getSumValues(), 0);
        assertEquals(0, statsEmptyMetadata.getSum2Values(), 0);
        assertEquals(0, statsEmptyMetadata.getMaxDiffValues(), 0);
        assertEquals(0, statsEmptyMetadata.getMinDiffValues(), 0);
        assertEquals(0, statsEmptyMetadata.getMaxDiffTs(), 0);
        assertEquals(0, statsEmptyMetadata.getMinDiffTs(), 0);
        ArrayList<HashMap<String, Object>> valuesToMonitor = statsEmptyMetadata.getValuesToMonitor();
        assertEquals(2, valuesToMonitor.size());
        assertEquals(monValue1, (Double) valuesToMonitor.get(0).get("mon_value"), 0);
        assertEquals(0, (Long) valuesToMonitor.get(0).get("num_occur"), 0);
        assertEquals(monValue2, (Double) valuesToMonitor.get(1).get("mon_value"), 0);
        assertEquals(0, (Long) valuesToMonitor.get(1).get("num_occur"), 0);
    } // testConstructorEmptyMetadata
    
    /**
     * Test of constructor method, of class OrionHDFSSink. Existent metadata is passed.
     */
    @Test
    public void testConstructorExistentMetadata() {
        System.out.println("Testing OrionStats.constructor (with existent metadata)");
        statsExistentMetadata = new OrionStats(attrName, attrType, existentMetadata, true, true, true,
                valuesToMonitorStr);
        assertEquals(attrName, statsExistentMetadata.getAttrName());
        assertEquals(attrType, statsExistentMetadata.getAttrType());
        assertEquals(prevValue, statsExistentMetadata.getPrevValue(), 0);
        assertEquals(prevTs, statsExistentMetadata.getPrevTs(), 0);
        assertEquals(numValues, statsExistentMetadata.getNumValues(), 0);
        assertEquals(maxValue, statsExistentMetadata.getMaxValue(), 0);
        assertEquals(minValue, statsExistentMetadata.getMinValue(), 0);
        assertEquals(sumValues, statsExistentMetadata.getSumValues(), 0);
        assertEquals(sum2Values, statsExistentMetadata.getSum2Values(), 0);
        assertEquals(maxDiffValues, statsExistentMetadata.getMaxDiffValues(), 0);
        assertEquals(minDiffValues, statsExistentMetadata.getMinDiffValues(), 0);
        assertEquals(maxDiffTs, statsExistentMetadata.getMaxDiffTs(), 0);
        assertEquals(minDiffTs, statsExistentMetadata.getMinDiffTs(), 0);
        ArrayList<HashMap<String, Object>> valuesToMonitor = statsExistentMetadata.getValuesToMonitor();
        assertEquals(2, valuesToMonitor.size());
        assertEquals(monValue1, (Double) valuesToMonitor.get(0).get("mon_value"), 0);
        assertEquals(numOccur1, (Long) valuesToMonitor.get(0).get("num_occur"), 0);
        assertEquals(monValue2, (Double) valuesToMonitor.get(1).get("mon_value"), 0);
        assertEquals(numOccur2, (Long) valuesToMonitor.get(1).get("num_occur"), 0);
    } // testConstructorExistentMetadata
    
    /**
     * Test of constructor method, of class OrionHDFSSink. Existent metadata is passed.
     */
    @Test
    public void testUpdate() {
        System.out.println("Testing OrionStats.update");
        statsExistentMetadata = new OrionStats(attrName, attrType, existentMetadata, true, true, true,
                valuesToMonitorStr);
        statsExistentMetadata.update(valueForUpdating, tsForUpdating);
        assertEquals(attrName, statsExistentMetadata.getAttrName());
        assertEquals(attrType, statsExistentMetadata.getAttrType());
        assertEquals(valueForUpdating, statsExistentMetadata.getPrevValue(), 0);
        assertEquals(tsForUpdating, statsExistentMetadata.getPrevTs(), 0);
        assertEquals(numValues + 1, statsExistentMetadata.getNumValues(), 0);
        assertEquals(maxValue, statsExistentMetadata.getMaxValue(), 0);
        assertEquals(minValue, statsExistentMetadata.getMinValue(), 0);
        assertEquals(sumValues + valueForUpdating, statsExistentMetadata.getSumValues(), 0);
        assertEquals(sum2Values + (valueForUpdating * valueForUpdating), statsExistentMetadata.getSum2Values(), 0);
        assertEquals(valueForUpdating - prevValue, statsExistentMetadata.getMaxDiffValues(), 0);
        assertEquals(minDiffValues, statsExistentMetadata.getMinDiffValues(), 0);
        assertEquals(maxDiffTs, statsExistentMetadata.getMaxDiffTs(), 0);
        assertEquals(tsForUpdating - prevTs, statsExistentMetadata.getMinDiffTs(), 0);
        ArrayList<HashMap<String, Object>> valuesToMonitor = statsExistentMetadata.getValuesToMonitor();
        assertEquals(2, valuesToMonitor.size());
        assertEquals(monValue1, (Double) valuesToMonitor.get(0).get("mon_value"), 0);
        assertEquals(numOccur1, (Long) valuesToMonitor.get(0).get("num_occur"), 0);
        assertEquals(monValue2, (Double) valuesToMonitor.get(1).get("mon_value"), 0);
        assertEquals(numOccur2 + 1, (Long) valuesToMonitor.get(1).get("num_occur"), 0);
    } // testUpdate
    
    /**
     * Test of constructor method, of class OrionHDFSSink. Existent metadata is passed.
     */
    @Test
    public void testToNGSIString() {
        System.out.println("Testing OrionStats.toNGSIString");
        statsExistentMetadata = new OrionStats(attrName, attrType, existentMetadata, true, true, true,
                valuesToMonitorStr);
        statsExistentMetadata.update(valueForUpdating, tsForUpdating);
        
        // if new stats can be created from the output of toNGSIString, then it is OK
        OrionStats stats = new OrionStats(attrName, attrType, statsExistentMetadata.toNGSIString(), true, true, true,
                valuesToMonitorStr);
        
        // the tests are the same than testUpdate ones
        assertEquals(attrName, stats.getAttrName());
        assertEquals(attrType, stats.getAttrType());
        assertEquals(valueForUpdating, stats.getPrevValue(), 0);
        assertEquals(tsForUpdating, stats.getPrevTs(), 0);
        assertEquals(numValues + 1, stats.getNumValues(), 0);
        assertEquals(maxValue, stats.getMaxValue(), 0);
        assertEquals(minValue, stats.getMinValue(), 0);
        assertEquals(sumValues + valueForUpdating, stats.getSumValues(), 0);
        assertEquals(sum2Values + (valueForUpdating * valueForUpdating), stats.getSum2Values(), 0);
        assertEquals(valueForUpdating - prevValue, stats.getMaxDiffValues(), 0);
        assertEquals(minDiffValues, stats.getMinDiffValues(), 0);
        assertEquals(maxDiffTs, stats.getMaxDiffTs(), 0);
        assertEquals(tsForUpdating - prevTs, stats.getMinDiffTs(), 0);
        ArrayList<HashMap<String, Object>> valuesToMonitor = stats.getValuesToMonitor();
        assertEquals(2, valuesToMonitor.size());
        assertEquals(monValue1, (Double) valuesToMonitor.get(0).get("mon_value"), 0);
        assertEquals(numOccur1, (Long) valuesToMonitor.get(0).get("num_occur"), 0);
        assertEquals(monValue2, (Double) valuesToMonitor.get(1).get("mon_value"), 0);
        assertEquals(numOccur2 + 1, (Long) valuesToMonitor.get(1).get("num_occur"), 0);
    } // testToNGSIString
    
} // OrionStatsTest
