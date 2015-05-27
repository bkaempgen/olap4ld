/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.io.Reader;
import java.io.InputStream;

import org.olap4j.*;
import org.olap4j.driver.olap4ld.EmptyResultSet;
import org.olap4j.driver.olap4ld.Factory;
import org.olap4j.driver.olap4ld.Olap4ldCellSet;
import org.olap4j.driver.olap4ld.Olap4ldConnection;
import org.olap4j.driver.olap4ld.Olap4ldStatement;
import org.olap4j.driver.olap4ld.Olap4ldPreparedStatement;
import org.olap4j.driver.olap4ld.proxy.XmlaOlap4jProxy;

/**
 * Implementation of {@link Factory} for JDBC 4.0.
 *
 * @author jhyde
 * @version $Id: FactoryJdbc4Impl.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Jun 14, 2007
 */
class FactoryJdbc4Impl implements Factory {
    /**
     * Creates a FactoryJdbc4Impl.
     */
    public FactoryJdbc4Impl() {
    }

    public Connection newConnection(
        Olap4ldDriver driver,
        XmlaOlap4jProxy proxy,
        String url,
        Properties info)
        throws SQLException
    {
        return new XmlaOlap4jConnectionJdbc4(
            this, driver, proxy, url, info);
    }

    public EmptyResultSet newEmptyResultSet(
        Olap4ldConnection olap4jConnection)
    {
        List<String> headerList = Collections.emptyList();
        List<List<Object>> rowList = Collections.emptyList();
        return new EmptyResultSetJdbc4(olap4jConnection, headerList, rowList);
    }

    public ResultSet newFixedResultSet(
        Olap4ldConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList)
    {
        return new EmptyResultSetJdbc4(
            olap4jConnection, headerList, rowList);
    }

    public Olap4ldCellSet newCellSet(
        Olap4ldStatement olap4jStatement) throws OlapException
    {
        return new XmlaOlap4jCellSetJdbc4(olap4jStatement);
    }

    public Olap4ldPreparedStatement newPreparedStatement(
        String mdx,
        Olap4ldConnection olap4jConnection) throws OlapException
    {
        return new XmlaOlap4jPreparedStatementJdbc4(olap4jConnection, mdx);
    }

    public Olap4ldDatabaseMetaData newDatabaseMetaData(
        Olap4ldConnection olap4jConnection)
    {
        return new XmlaOlap4jDatabaseMetaDataJdbc4(olap4jConnection);
    }

    // Inner classes

    private static class EmptyResultSetJdbc4 extends EmptyResultSet {
        /**
         * Creates a EmptyResultSetJdbc4.
         *
         * @param olap4jConnection Connection
         * @param headerList Column names
         * @param rowList List of row values
         */
        EmptyResultSetJdbc4(
            Olap4ldConnection olap4jConnection,
            List<String> headerList,
            List<List<Object>> rowList)
        {
            super(olap4jConnection, headerList, rowList);
        }

        // implement java.sql.ResultSet methods
        // introduced in JDBC 4.0/JDK 1.6

        public RowId getRowId(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public RowId getRowId(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateRowId(int columnIndex, RowId x) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateRowId(String columnLabel, RowId x)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public int getHoldability() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public boolean isClosed() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateNString(
            int columnIndex, String nString) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNString(
            String columnLabel, String nString) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            String columnLabel, NClob nClob) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public NClob getNClob(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public NClob getNClob(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public SQLXML getSQLXML(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public SQLXML getSQLXML(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateSQLXML(
            int columnIndex, SQLXML xmlObject) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateSQLXML(
            String columnLabel, SQLXML xmlObject) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public String getNString(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public String getNString(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Reader getNCharacterStream(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Reader getNCharacterStream(String columnLabel)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            int columnIndex, Reader x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            String columnLabel, Reader reader, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            int columnIndex, InputStream x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            int columnIndex, InputStream x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            int columnIndex, Reader x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            String columnLabel, InputStream x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            String columnLabel, InputStream x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            String columnLabel, Reader reader, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            int columnIndex,
            InputStream inputStream,
            long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            String columnLabel,
            InputStream inputStream,
            long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            int columnIndex, Reader reader, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            String columnLabel, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            int columnIndex, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            String columnLabel, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            int columnIndex, Reader x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            int columnIndex, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            int columnIndex, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            int columnIndex, Reader x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            String columnLabel, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            String columnLabel, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            int columnIndex, InputStream inputStream) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            String columnLabel, InputStream inputStream) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            int columnIndex, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            int columnIndex, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

		@Override
		public <T> T getObject(int columnIndex, Class<T> type)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T> T getObject(String columnLabel, Class<T> type)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}
    }

    private static class XmlaOlap4jConnectionJdbc4
        extends Olap4ldConnection
        implements OlapConnection
    {
        /**
         * Creates a XmlaOlap4jConnectionJdbc4.
         *
         * @param factory Factory
         * @param driver Driver
         * @param proxy Proxy
         * @param url URL
         * @param info Extra properties
         * @throws SQLException on error
         */
        public XmlaOlap4jConnectionJdbc4(
            Factory factory,
            Olap4ldDriver driver,
            XmlaOlap4jProxy proxy,
            String url,
            Properties info) throws SQLException
        {
            super(factory, driver, proxy, url, info);
        }

        public OlapStatement createStatement() {
            return super.createStatement();
        }

        public OlapDatabaseMetaData getMetaData() {
            return super.getMetaData();
        }

        // implement java.sql.Connection methods
        // introduced in JDBC 4.0/JDK 1.6

        public Clob createClob() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Blob createBlob() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public NClob createNClob() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public SQLXML createSQLXML() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public boolean isValid(int timeout) throws SQLException {
            return !isClosed();
        }

        public void setClientInfo(
            String name, String value) throws SQLClientInfoException
        {
            throw new UnsupportedOperationException();
        }

        public void setClientInfo(Properties properties)
            throws SQLClientInfoException
        {
            throw new UnsupportedOperationException();
        }

        public String getClientInfo(String name) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Properties getClientInfo() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Array createArrayOf(
            String typeName, Object[] elements) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public Struct createStruct(
            String typeName, Object[] attributes) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

		@Override
		public void abort(Executor executor) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getNetworkTimeout() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void setNetworkTimeout(Executor executor, int milliseconds)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}
    }

    private static class XmlaOlap4jCellSetJdbc4 extends Olap4ldCellSet {
        /**
         * Creates an XmlaOlap4jCellSetJdbc4.
         *
         * @param olap4jStatement Statement
         * @throws OlapException on error
         */
        XmlaOlap4jCellSetJdbc4(
            Olap4ldStatement olap4jStatement)
            throws OlapException
        {
            super(olap4jStatement);
        }

        public CellSetMetaData getMetaData() {
            return super.getMetaData();
        }

        // implement java.sql.CellSet methods
        // introduced in JDBC 4.0/JDK 1.6

        public RowId getRowId(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public RowId getRowId(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateRowId(int columnIndex, RowId x) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateRowId(String columnLabel, RowId x)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public int getHoldability() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public boolean isClosed() throws SQLException {
            return closed;
        }

        public void updateNString(
            int columnIndex, String nString) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNString(
            String columnLabel, String nString) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            String columnLabel, NClob nClob) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public NClob getNClob(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public NClob getNClob(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public SQLXML getSQLXML(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public SQLXML getSQLXML(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void updateSQLXML(
            int columnIndex, SQLXML xmlObject) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateSQLXML(
            String columnLabel, SQLXML xmlObject) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public String getNString(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public String getNString(String columnLabel) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Reader getNCharacterStream(int columnIndex) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public Reader getNCharacterStream(String columnLabel)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            int columnIndex, Reader x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            String columnLabel, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            int columnIndex, InputStream x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            int columnIndex, InputStream x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            int columnIndex, Reader x, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            String columnLabel, InputStream x, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            String columnLabel, InputStream x, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            String columnLabel, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            int columnIndex,
            InputStream inputStream,
            long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            String columnLabel,
            InputStream inputStream,
            long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            int columnIndex, Reader reader, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            String columnLabel, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            int columnIndex, Reader reader, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            String columnLabel, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            int columnIndex, Reader x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNCharacterStream(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            int columnIndex, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            int columnIndex, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            int columnIndex, Reader x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateAsciiStream(
            String columnLabel, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBinaryStream(
            String columnLabel, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateCharacterStream(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            int columnIndex, InputStream inputStream) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateBlob(
            String columnLabel, InputStream inputStream) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(int columnIndex, Reader reader)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateClob(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            int columnIndex, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void updateNClob(
            String columnLabel, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

		@Override
		public <T> T getObject(int columnIndex, Class<T> type)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T> T getObject(String columnLabel, Class<T> type)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}
    }

    private static class XmlaOlap4jPreparedStatementJdbc4
        extends Olap4ldPreparedStatement
    {
        /**
         * Creates a XmlaOlap4jPreparedStatementJdbc4.
         *
         * @param olap4jConnection Connection
         * @param mdx MDX query text
         * @throws OlapException on error
         */
        XmlaOlap4jPreparedStatementJdbc4(
            Olap4ldConnection olap4jConnection,
            String mdx) throws OlapException
        {
            super(olap4jConnection, mdx);
        }

        public CellSetMetaData getMetaData() {
            return super.getMetaData();
        }

        // implement java.sql.PreparedStatement methods
        // introduced in JDBC 4.0/JDK 1.6

        public void setRowId(int parameterIndex, RowId x) throws SQLException {
            throw new UnsupportedOperationException();
        }

        public void setNString(
            int parameterIndex, String value) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setNCharacterStream(
            int parameterIndex, Reader value, long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setNClob(int parameterIndex, NClob value)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setClob(
            int parameterIndex, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setBlob(
            int parameterIndex,
            InputStream inputStream,
            long length) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setNClob(
            int parameterIndex, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setSQLXML(
            int parameterIndex, SQLXML xmlObject) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setAsciiStream(
            int parameterIndex, InputStream x, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setBinaryStream(
            int parameterIndex, InputStream x, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setCharacterStream(
            int parameterIndex, Reader reader, long length)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setAsciiStream(
            int parameterIndex, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setBinaryStream(
            int parameterIndex, InputStream x) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setCharacterStream(
            int parameterIndex, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setNCharacterStream(
            int parameterIndex, Reader value) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setClob(int parameterIndex, Reader reader)
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setBlob(
            int parameterIndex, InputStream inputStream) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setNClob(
            int parameterIndex, Reader reader) throws SQLException
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class XmlaOlap4jDatabaseMetaDataJdbc4
        extends Olap4ldDatabaseMetaData
    {
        /**
         * Creates an XmlaOlap4jDatabaseMetaDataJdbc4.
         *
         * @param olap4jConnection Connection
         */
        XmlaOlap4jDatabaseMetaDataJdbc4(
            Olap4ldConnection olap4jConnection)
        {
            super(olap4jConnection);
        }

        public OlapConnection getConnection() {
            return super.getConnection();
        }

        // implement java.sql.DatabaseMetaData methods
        // introduced in JDBC 4.0/JDK 1.6

        public RowIdLifetime getRowIdLifetime() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public ResultSet getSchemas(
            String catalog, String schemaPattern) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public ResultSet getClientInfoProperties() throws SQLException {
            throw new UnsupportedOperationException();
        }

        public ResultSet getFunctions(
            String catalog,
            String schemaPattern,
            String functionNamePattern) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public ResultSet getFunctionColumns(
            String catalog,
            String schemaPattern,
            String functionNamePattern,
            String columnNamePattern) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

		@Override
		public boolean generatedKeyAlwaysReturned() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public ResultSet getPseudoColumns(String arg0, String arg1,
				String arg2, String arg3) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}
    }
}

// End FactoryJdbc4Impl.java
