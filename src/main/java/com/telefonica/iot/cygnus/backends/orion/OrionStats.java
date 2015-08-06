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

import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.sinks.OrionStatsSink;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class OrionStats {
    
    private static final CygnusLogger LOGGER = new CygnusLogger(OrionStatsSink.class);
    private String attrName;
    private String attrType;
    private boolean enableValueAnalysis;
    private boolean enableDiffAnalysis;
    private boolean enableClusteringAnalysis;
    private double prevValue;
    private long prevTs;
    private long numValues;
    private double maxValue;
    private double minValue;
    private double sumValues;
    private double sum2Values;
    private double maxDiffValues;
    private double minDiffValues;
    private double maxDiffTs;
    private double minDiffTs;
    private ArrayList<Double> clusters;
    private ArrayList<HashMap<String, Object>> valuesToMonitor;
    
    /**
     * Constructor.
     * @param attrName
     * @param attrType
     * @param attrMetadata
     * @param enableValueAnalysis
     * @param enableDiffAnalysis
     * @param enableClusteringAnalysis
     * @param valuesToMonitorStr
     */
    public OrionStats(String attrName, String attrType, String attrMetadata, boolean enableValueAnalysis,
            boolean enableDiffAnalysis, boolean enableClusteringAnalysis, String valuesToMonitorStr) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.enableValueAnalysis = enableValueAnalysis;
        this.enableDiffAnalysis = enableDiffAnalysis;
        this.enableClusteringAnalysis = enableClusteringAnalysis;
        prevValue = Double.POSITIVE_INFINITY;
        prevTs = Long.MAX_VALUE;
        numValues = 0;
        maxValue = Double.MIN_VALUE;
        minValue = Double.MAX_VALUE;
        sumValues = 0;
        sum2Values = 0;
        maxDiffValues = 0;
        minDiffValues = 0;
        maxDiffTs = 0;
        minDiffTs = 0;
        clusters = new ArrayList<Double>();
        valuesToMonitor = new ArrayList<HashMap<String, Object>>();
        
        if (valuesToMonitorStr != null) {
            String[] values = valuesToMonitorStr.split(",");
        
            for (String value : values) {
                HashMap map = new HashMap<String, Long>();
                map.put("mon_value", new Double(value));
                map.put("num_occur", (long) 0);
                valuesToMonitor.add(map);
            } // for
        } // if
        
        // metadata are written in JSON format, thus parse it
        if (attrMetadata == null) {
            LOGGER.debug("Null metadata, it will be ignored. Returning default values");
            return;
        } // if
        
        JSONParser jsonParser = new JSONParser();
        JSONArray mdArray;
        
        try {
            mdArray = ((JSONArray) jsonParser.parse(attrMetadata));
        } catch (ParseException e) {
            LOGGER.error("Error while parsing metadata. Returning default values. Details=" + e.getMessage());
            return;
        } // try catch
        
        for (Object mdObject : mdArray) {
            JSONObject md = (JSONObject) mdObject;
            String mdName = (String) md.get("name");
            
            if (mdName != null && mdName.equals("orion_stats_sink_prev_value")) {
                prevValue = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_prev_ts")) {
                prevTs = (Long) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_num_values")) {
                numValues = (Long) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_max_value")) {
                maxValue = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_min_value")) {
                minValue = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_sum_values")) {
                sumValues = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_sum2_values")) {
                sum2Values = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_max_diff_values")) {
                maxDiffValues = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_min_diff_values")) {
                minDiffValues = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_max_diff_ts")) {
                maxDiffTs = (Double) md.get("value");
            } else if (mdName != null && mdName.equals("orion_stats_sink_min_diff_ts")) {
                minDiffTs = (Double) md.get("value");
/*
            TBD
            }  else if (mdName != null && mdName.equals("orion_stats_sink_clusters")) {
                clusters = (ArrayList<Double>) md.get("value");
*/
            } else if (mdName != null && mdName.equals("orion_stats_sink_values_to_monitor")) {
                valuesToMonitor = (ArrayList<HashMap<String, Object>>) md.get("value");
            } // if else if
        } // for
        
        LOGGER.debug("Statistics created for attrName=" + attrName + " and attrType=" + attrType + ": "
                + prevValue + "," + prevTs + "," + numValues + "," + maxValue + "," + minValue + "," + sumValues
                + "," + sum2Values + "," + maxDiffValues + "," + minDiffValues + "," + maxDiffTs + "," + minDiffTs
                + "," + clusters.toString() + "," + valuesToMonitor.toString());
    } // OrionStats
    
    /**
     * Gets the attribute name this statistic is for.
     * @return
     */
    public String getAttrName() {
        return attrName;
    } // getAttrName
    
    /**
     * Gets the attribute type this statistic is for.
     * @return
     */
    public String getAttrType() {
        return attrType;
    } // getAttrType
    
    /**
     * Gets the previous value.
     * @return
     */
    public double getPrevValue() {
        return prevValue;
    } // getPrevValue
    
    /**
     * Gets the previous timestamp.
     * @return
     */
    public double getPrevTs() {
        return prevTs;
    } // getPrevTs
    
    /**
     * Gets the number of values.
     * @return
     */
    public double getNumValues() {
        return numValues;
    } // getNumValues
    
    /**
     * Gets the maximum value.
     * @return
     */
    public double getMaxValue() {
        return maxValue;
    } // getMaxValue
    
    /**
     * Gets the minimum value.
     * @return
     */
    public double getMinValue() {
        return minValue;
    } // getMinValue
    
    /**
     * Gets the sum of the values.
     * @return
     */
    public double getSumValues() {
        return sumValues;
    } // getSumValues
    
    /**
     * Gets the sum^2 of the values.
     * @return
     */
    public double getSum2Values() {
        return sum2Values;
    } // getSum2Values
    
    /**
     * Gets the maximum difference among two consecutive values.
     * @return
     */
    public double getMaxDiffValues() {
        return maxDiffValues;
    } // getMaxDiffValues
    
    /**
     * Gets the minimum difference among two consecutive values.
     * @return
     */
    public double getMinDiffValues() {
        return minDiffValues;
    } // getMinDiffValues
    
    /**
     * Gets the maximum difference among two consecutive timestamps.
     * @return
     */
    public double getMaxDiffTs() {
        return maxDiffTs;
    } // getMaxDiffTs
    
    /**
     * Gets the minimum difference among two consecutive timestamps.
     * @return
     */
    public double getMinDiffTs() {
        return minDiffTs;
    } // getMinDiffTs
    
    ArrayList<HashMap<String, Object>> getValuesToMonitor() {
        return this.valuesToMonitor;
    } // getValuesToMonitor
    
    /**
     * Update the stats given the new value and timestamp.
     * @param value
     * @param ts
     */
    public void update(double value, long ts) {
        if (enableValueAnalysis) {
            numValues++;

            if (value > maxValue) {
                maxValue = value;
            } // if else if

            if (value < minValue) {
                minValue = value;
            } // if

            sumValues += value;
            sum2Values += (value * value);
        } // if
        
        if (enableDiffAnalysis) {
            if (prevValue != Double.POSITIVE_INFINITY) {
                double diffValue = Math.abs(value - prevValue);

                if (diffValue > maxDiffValues) {
                    maxDiffValues = diffValue;
                } // if

                if (diffValue < minDiffValues) {
                    minDiffValues = diffValue;
                } // if
            } // if

            if (prevTs != Long.MAX_VALUE) {
                double diffTs = Math.abs(ts - prevTs);

                if (diffTs > maxDiffTs) {
                    maxDiffTs = diffTs;
                } // if

                if (diffTs < minDiffTs) {
                    minDiffTs = diffTs;
                } // if
            } // if

            prevValue = value;
            prevTs = ts;
        } // if
        
/*
        TBD
        if (enableClusteringAnalysis) {
        } // if
*/

        if (valuesToMonitor.size() > 0) {
            for (HashMap<String, Object> valueToMonitor: valuesToMonitor) {
                Double monValue = (Double) valueToMonitor.get("mon_value");
                
                if (monValue == value) {
                    valueToMonitor.put("num_occur", (Long) valueToMonitor.get("num_occur") + 1);
                    break;
                } // if
            } // for
        } // if
        
        LOGGER.debug("Statistics updated for attrName=" + attrName + " and attrType=" + attrType + ": "
                + prevValue + "," + prevTs + "," + numValues + "," + maxValue + "," + minValue + "," + sumValues
                + "," + sum2Values + "," + maxDiffValues + "," + minDiffValues + "," + maxDiffTs + "," + minDiffTs
                + "," + clusters.toString() + "," + valuesToMonitor.toString());
    } // update
    
    /**
     * Serializes the statistics as a NGSI-like JSON attribute.
     * @return
     */
    public String toNGSIString() {
        String ngsiStr = ""
                + "             [";
                
        if (enableValueAnalysis) {
            ngsiStr += ""
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_prev_value\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + prevValue
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_prev_ts\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + prevTs
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_num_values\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + numValues
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_max_value\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + maxValue
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_min_value\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + minValue
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_sum_values\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + sumValues
                    + "                },";
        } // if
        
        if (enableDiffAnalysis) {
            ngsiStr += ""
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_sum2_values\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + sum2Values
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_max_diff_values\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + maxDiffValues
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_min_diff_values\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + minDiffValues
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_max_diff_ts\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + maxDiffTs
                    + "                },"
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_min_diff_ts\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + minDiffTs
                    + "                },";
        } // if
/*
        TBD
        if (enableClusteringAnalysis) {
            ngsiStr += ""
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_clusters\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + JSONValue.toJSONString(clusters)
                    + "                },";
        } // if
*/
        if (valuesToMonitor.size() > 0) {
            ngsiStr += ""
                    + "                {"
                    + "                   \"name\": \"orion_stats_sink_values_to_monitor\","
                    + "                   \"type\": \"double\","
                    + "                   \"value\": " + JSONValue.toJSONString(valuesToMonitor)
                    + "                }";
        } // if
        
        if (ngsiStr.charAt(ngsiStr.length() - 1) == ',') {
            ngsiStr = ngsiStr.substring(0, ngsiStr.length());
        } // if
        
        ngsiStr += ""
                + "             ]";
        return ngsiStr;
    } // toNGSIString
    
} // OrionStats
