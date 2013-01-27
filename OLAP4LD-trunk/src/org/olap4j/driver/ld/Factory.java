/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld;

import org.olap4j.OlapException;
import org.olap4j.driver.ld.proxy.XmlaOlap4jProxy;
import org.olap4j.driver.ld.EmptyResultSet;
import org.olap4j.driver.ld.LdOlap4jCellSet;
import org.olap4j.driver.ld.LdOlap4jConnection;
import org.olap4j.driver.ld.XmlaOlap4jPreparedStatement;
import org.olap4j.driver.ld.LdOlap4jStatement;

import java.sql.*;
import java.util.Properties;
import java.util.List;

/**
 * Instantiates classes to implement the olap4j API against the
 * an XML for Analysis provider.
 *
 * <p>There are implementations for JDBC 3.0 (which occurs in JDK 1.5)
 * and JDBC 4.0 (which occurs in JDK 1.6).
 *
 * @author jhyde
 * @version $Id: Factory.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Jun 14, 2007
 */
interface Factory {
    /**
     * Creates a connection.
     *
     * @param driver Driver
     * @param proxy Proxy (for submitting requests, via HTTP or otherwise)
     * @param url URL of server
     * @param info Properties defining the connection
     * @return Connection
     * @throws SQLException on error
     */
    Connection newConnection(
        LdOlap4jDriver driver,
        XmlaOlap4jProxy proxy,
        String url,
        Properties info) throws SQLException;

    /**
     * Creates an empty result set.
     *
     * @param olap4jConnection Connection
     * @return Result set
     */
    EmptyResultSet newEmptyResultSet(
        LdOlap4jConnection olap4jConnection);

    /**
     * Creates a result set with a fixed set of rows.
     *
     * @param olap4jConnection Connection
     * @param headerList Column headers
     * @param rowList Row values
     * @return Result set
     */
    ResultSet newFixedResultSet(
        LdOlap4jConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList);

    /**
     * Creates a cell set.
     *
     * @param olap4jStatement Statement
     * @return Cell set
     * @throws OlapException on error
     */
    LdOlap4jCellSet newCellSet(
        LdOlap4jStatement olap4jStatement) throws OlapException;

    /**
     * Creates a prepared statement.
     *
     * @param mdx MDX query text
     * @param olap4jConnection Connection
     * @return Prepared statement
     * @throws OlapException on error
     */
    XmlaOlap4jPreparedStatement newPreparedStatement(
        String mdx,
        LdOlap4jConnection olap4jConnection) throws OlapException;

    /**
     * Creates a metadata object.
     *
     * @param olap4jConnection Connection
     * @return Metadata object
     */
    LdOlap4jDatabaseMetaData newDatabaseMetaData(
        LdOlap4jConnection olap4jConnection);
}

// End Factory.java
