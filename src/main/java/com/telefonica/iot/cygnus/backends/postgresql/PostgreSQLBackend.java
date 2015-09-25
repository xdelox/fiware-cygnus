/**
 * Copyright 2015 Telefonica Investigación y Desarrollo Chile
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

package com.telefonica.iot.cygnus.backends.postgresql;

import com.telefonica.iot.cygnus.errors.CygnusBadContextData;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import com.telefonica.iot.cygnus.utils.Constants;
import java.sql.SQLTimeoutException;
import java.util.HashMap;
import java.util.Properties;


/**
 *
 * @author cristobalcastillo
 */
public class PostgreSQLBackend {

    private static final String DRIVER_NAME = "org.postgresql.Driver";
    private final String postgresqlHost;
    private final String postgresqlPort;
    private final String postgresqlUsername;
    private final String postgresqlPassword;
    private final HashMap<String, Connection> connections;
    private PostgreSQLDriver driver;
    private static final CygnusLogger LOGGER = new CygnusLogger(PostgreSQLBackend.class);

    /**
     * Constructor.
     * @param postgresqlHost
     * @param postgresqlPort
     * @param postgresqlUsername
     * @param postgresqlPassword
     */
    public PostgreSQLBackend(String postgresqlHost, String postgresqlPort, String postgresqlUsername, String postgresqlPassword) {
        this.postgresqlHost = postgresqlHost;
        this.postgresqlPort = postgresqlPort;
        this.postgresqlUsername = postgresqlUsername;
        this.postgresqlPassword = postgresqlPassword;
        connections = new HashMap<String, Connection>();
        driver = new PostgreSQLDriver();
    } // PostgreSQLBackend

    /**
     * Gets the map of database name-connection. It is protected since it is only used by the tests.
     * @return The mao of database name-connection.
     */
    protected HashMap<String, Connection> getConnections() {
        return connections;
    } // getCnnections

    /**
     * Sets the PostgreSQL driver. It is protected since it is only used by the tests.
     * @param driver The PostgreSQL driver to be set.
     */
    protected void setDriver(PostgreSQLDriver driver) {
        this.driver = driver;
    } // setDriver

    /**
     * Creates a database schema, given its name, if not exists.
     * @param schemaName
     * @throws Exception
     */
    public void createDatabaseSchema(String schemaName) throws Exception {
        Statement stmt = null;

        // get a connection to an empty database
        Connection con = getConnection("");

        try {
            stmt = con.createStatement();
        } catch (Exception e) {
            throw new CygnusRuntimeError(e.getMessage());
        } // try catch

        try {
            String query = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
            LOGGER.debug("Executing SQL query '" + query + "'");
            stmt.executeUpdate(query);
        } catch (Exception e) {
            throw new CygnusRuntimeError(e.getMessage());
        } // try catch

        closePostgreSQLObjects(con, stmt);
    } // createDatabaseSchema

    /**
     * Creates a table, given its name, if not exists in the given schema.
     * @param schema
     * @param tableName
     * @throws Exception
     */
    public void createTable(String schema, String tableName) throws Exception {
        Statement stmt = null;

        // get a connection to the given database
        Connection con = getConnection("");

        try {
            stmt = con.createStatement();
        } catch (Exception e) {
            throw new CygnusRuntimeError(e.getMessage());
        } // try catch

        try {
            String query = "CREATE TABLE IF NOT EXISTS "
                           + schema + "." + tableName + " ("
                           + Constants.RECV_TIME_TS + " bigint, "
                           + Constants.RECV_TIME + " text, "
                           + Constants.ENTITY_ID + " text, "
                           + Constants.ENTITY_TYPE + " text, "
                           + Constants.ATTR_NAME + " text, "
                           + Constants.ATTR_TYPE + " text, "
                           + Constants.ATTR_VALUE + " text, "
                           + Constants.ATTR_MD + " text)";
            LOGGER.debug("Executing SQL query '" + query + "'");
            stmt.executeUpdate(query);
        } catch (Exception e) {
            throw new CygnusRuntimeError(e.getMessage());
        } // try catch

        closePostgreSQLObjects(con, stmt);
    } // createTable

    /**
     * Inserts a new row in the given table within the given database representing a unique attribute change.
     * @param schema
     * @param tableName
     * @param recvTimeTs
     * @param recvTime
     * @param entityId
     * @param entityType
     * @param attrName
     * @param attrType
     * @param attrValue
     * @param attrMd
     * @throws Exception
     */
    public void insertContextData(String schema, String tableName, long recvTimeTs, String recvTime, String entityId,
                                  String entityType, String attrName, String attrType, String attrValue, String attrMd) throws Exception {
        Statement stmt = null;

        Connection con = getConnection("");

        try {
            stmt = con.createStatement();
        } catch (Exception e) {
            throw new CygnusRuntimeError(e.getMessage());
        } // try catch

        try {
            String query = "INSERT INTO " + schema + "." + tableName + " VALUES ('" + recvTimeTs + "', '" + recvTime + "', '"
                           + entityId + "', '" + entityType + "', '" + attrName + "', '" + attrType + "', '" + attrValue
                           + "', '" + attrMd + "')";
            LOGGER.debug("Executing PostgreSQL query '" + query + "'");
            stmt.executeUpdate(query);
        } catch (SQLTimeoutException e) {
            throw new CygnusPersistenceError(e.getMessage());
        } catch (SQLException e) {
            throw new CygnusBadContextData(e.getMessage());
        } // try catch

        closePostgreSQLObjects(con, stmt);
    } // insertContextData

    /**
     * Inserts a new row in the given table within the given database representing full attribute list changes.
     * @param schema
     * @param tableName
     * @param recvTime
     * @param attrs
     * @param mds
     * @throws Exception
     */
    public void insertContextData(String schema, String tableName, String recvTime,
                                  Map<String, String> attrs, Map<String, String> mds) throws Exception {
        Statement stmt = null;
        String columnNames = null;
        String columnValues = null;

        // get a connection to the PostgreSQL server and get a statement
        Connection con = getConnection("");

        try {

            stmt = con.createStatement();

            // for query building purposes
            columnNames = Constants.RECV_TIME;
            columnValues = "'" + recvTime + "'";

            for (String attrName : attrs.keySet()) {
                columnNames += "," + attrName;
                String attrValue = attrs.get(attrName);
                columnValues += ",'" + attrValue + "'";
            } // for

            for (String attrMdName : mds.keySet()) {
                columnNames += "," + attrMdName;
                String md = mds.get(attrMdName);
                columnValues += ",'" + md + "'";
            } // for
        } catch (Exception e) {
            throw new CygnusRuntimeError(e.getMessage());
        } // try catch

        try {
            // finish creating the query and execute it
            String query = "insert into " + schema + "." + tableName + " (" + columnNames + ") values (" + columnValues + ")";
            LOGGER.debug("Executing SQL query '" + query + "'");
            stmt.executeUpdate(query);
        } catch (SQLTimeoutException e) {
            throw new CygnusPersistenceError(e.getMessage());
        } catch (SQLException e) {
            throw new CygnusBadContextData(e.getMessage());
        } // try catch

        closePostgreSQLObjects(con, stmt);
    } // insertContextData

    /**
     * Gets a connection to the PostgreSQL server.
     * @return
     * @throws Exception
     */
    private Connection getConnection(String schema) throws Exception {
        try {
            // FIXME: the number of cached connections should be limited to a certain number; with such a limit
            //        number, if a new connection is needed, the oldest one is closed
            Connection con = connections.get(schema);

            if (con == null || !con.isValid(0)) {
                if (con != null) {
                    con.close();
                }
                con = driver.getConnection(postgresqlHost,
                                           postgresqlPort, schema, postgresqlUsername, postgresqlPassword);
                connections.put(schema, con);
            } // if

            return con;
        } catch (ClassNotFoundException e) {
            throw new CygnusPersistenceError(e.getMessage());
        } catch (SQLException e) {
            throw new CygnusPersistenceError(e.getMessage());
        } // try catch
    } // getConnection

    /**
     * Close all the PostgreSQL objects previously opened by doCreateTable and doQuery.
     * @param con
     * @param stmt
     * @return True if the PostgreSQL objects have been closed, false otherwise.
     */
    private void closePostgreSQLObjects(Connection con, Statement stmt) throws Exception {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                throw new CygnusRuntimeError("The PostgreSQL connection could not be closed. Details="
                                             + e.getMessage());
            } // try catch
        } // if

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new CygnusRuntimeError("The PostgreSQL statement could not be closed. Details="
                                             + e.getMessage());
            } // try catch
        } // if
    } // closePostgreSQLObjects

    /**
     * This code has been extracted from PostgreSQLBackend.getConnection() for testing purposes. By extracting it into a
     * class then it can be mocked.
     */
    protected class PostgreSQLDriver {
        /**
         * Gets a psql connection.
         * @param host
         * @param port
         * @param schema
         * @param user
         * @param password
         * @return A psql connection
         * @throws Exception
         */
        Connection getConnection(String host, String port, String schema, String user, String password)
        throws Exception {
            // dynamically load the PostgreSQL JDBC driver
            Class.forName(DRIVER_NAME);

            // return a connection based on the PostgreSQL JDBC driver
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + schema;
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("sslmode", "disable");

            LOGGER.debug("Connecting to" + url);
            return DriverManager.getConnection(url, props);
        } // getConnection
    } // PostgreSQLDriver
} // PostgreSQLBackend
