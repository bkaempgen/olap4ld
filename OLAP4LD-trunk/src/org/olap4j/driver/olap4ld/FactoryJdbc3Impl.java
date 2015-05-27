/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld;

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.EmptyResultSet;
import org.olap4j.driver.olap4ld.Factory;
import org.olap4j.driver.olap4ld.FactoryJdbc3Impl;
import org.olap4j.driver.olap4ld.Olap4ldCellSet;
import org.olap4j.driver.olap4ld.Olap4ldConnection;
import org.olap4j.driver.olap4ld.Olap4ldStatement;
import org.olap4j.driver.olap4ld.Olap4ldPreparedStatement;
import org.olap4j.driver.olap4ld.proxy.XmlaOlap4jProxy;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link Factory} for JDBC 3.0.
 *
 * @author jhyde
 * @version $Id: FactoryJdbc3Impl.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Jun 14, 2007
 */
class FactoryJdbc3Impl implements Factory {
    /**
     * Creates a FactoryJdbc3Impl.
     */
    public FactoryJdbc3Impl() {
    }

    public Connection newConnection(
        Olap4ldDriver driver,
        XmlaOlap4jProxy proxy,
        String url,
        Properties info)
        throws SQLException
    {
        return new FactoryJdbc3Impl.XmlaOlap4jConnectionJdbc3(
            driver, proxy, url, info);
    }

    public EmptyResultSet newEmptyResultSet(
        Olap4ldConnection olap4jConnection)
    {
        List<String> headerList = Collections.emptyList();
        List<List<Object>> rowList = Collections.emptyList();
        return new FactoryJdbc3Impl.EmptyResultSetJdbc3(
            olap4jConnection, headerList, rowList);
    }

    public ResultSet newFixedResultSet(
        Olap4ldConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList)
    {
        return new EmptyResultSetJdbc3(olap4jConnection, headerList, rowList);
    }

    public Olap4ldCellSet newCellSet(
        Olap4ldStatement olap4jStatement) throws OlapException
    {
        return new FactoryJdbc3Impl.XmlaOlap4jCellSetJdbc3(
            olap4jStatement);
    }

    public Olap4ldPreparedStatement newPreparedStatement(
        String mdx,
        Olap4ldConnection olap4jConnection) throws OlapException
    {
        return new FactoryJdbc3Impl.XmlaOlap4jPreparedStatementJdbc3(
            olap4jConnection, mdx);
    }

    public Olap4ldDatabaseMetaData newDatabaseMetaData(
        Olap4ldConnection olap4jConnection)
    {
        return new FactoryJdbc3Impl.XmlaOlap4jDatabaseMetaDataJdbc3(
            olap4jConnection);
    }

    // Inner classes

    private static class XmlaOlap4jPreparedStatementJdbc3
        extends Olap4ldPreparedStatement
    {
        public XmlaOlap4jPreparedStatementJdbc3(
            Olap4ldConnection olap4jConnection,
            String mdx) throws OlapException
        {
            super(olap4jConnection, mdx);
        }

		@Override
		public void setAsciiStream(int arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setAsciiStream(int arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setBinaryStream(int arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setBinaryStream(int arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setBlob(int arg0, InputStream arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setBlob(int arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setCharacterStream(int arg0, Reader arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setCharacterStream(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setClob(int arg0, Reader arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setClob(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setNCharacterStream(int arg0, Reader arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setNCharacterStream(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setNClob(int arg0, NClob arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setNClob(int arg0, Reader arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setNClob(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setNString(int arg0, String arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setRowId(int arg0, RowId arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}
    }

    private static class XmlaOlap4jCellSetJdbc3
        extends Olap4ldCellSet
    {
        public XmlaOlap4jCellSetJdbc3(
            Olap4ldStatement olap4jStatement) throws OlapException
        {
            super(olap4jStatement);
        }

		@Override
		public int getHoldability() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Reader getNCharacterStream(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Reader getNCharacterStream(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NClob getNClob(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NClob getNClob(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getNString(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getNString(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RowId getRowId(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RowId getRowId(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLXML getSQLXML(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLXML getSQLXML(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isClosed() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void updateAsciiStream(int arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateAsciiStream(String arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateAsciiStream(int arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateAsciiStream(String arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(int arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(String arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(int arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(String arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(int arg0, InputStream arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(String arg0, InputStream arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(int arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(String arg0, InputStream arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(int arg0, Reader arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(String arg0, Reader arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(String arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(int arg0, Reader arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(String arg0, Reader arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(String arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(int arg0, Reader arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(String arg0, Reader arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(String arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(int arg0, NClob arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(String arg0, NClob arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(int arg0, Reader arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(String arg0, Reader arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(int arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(String arg0, Reader arg1, long arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNString(int arg0, String arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNString(String arg0, String arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateRowId(int arg0, RowId arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateRowId(String arg0, RowId arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
			// TODO Auto-generated method stub
			
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

    private static class EmptyResultSetJdbc3 extends EmptyResultSet {
        public EmptyResultSetJdbc3(
            Olap4ldConnection olap4jConnection,
            List<String> headerList,
            List<List<Object>> rowList)
        {
            super(olap4jConnection, headerList, rowList);
        }

		@Override
		public int getHoldability() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Reader getNCharacterStream(int columnIndex) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Reader getNCharacterStream(String columnLabel)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NClob getNClob(int columnIndex) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NClob getNClob(String columnLabel) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getNString(int columnIndex) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getNString(String columnLabel) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RowId getRowId(int columnIndex) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RowId getRowId(String columnLabel) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLXML getSQLXML(int columnIndex) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLXML getSQLXML(String columnLabel) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isClosed() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(int columnIndex, InputStream inputStream)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(String columnLabel, InputStream inputStream)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(int columnIndex, InputStream inputStream,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateBlob(String columnLabel, InputStream inputStream,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x, long length)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(int columnIndex, Reader reader)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(String columnLabel, Reader reader)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(int columnIndex, Reader reader, long length)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateClob(String columnLabel, Reader reader, long length)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(int columnIndex, Reader x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(String columnLabel, Reader reader)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(int columnIndex, Reader x,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNCharacterStream(String columnLabel, Reader reader,
				long length) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(int columnIndex, NClob nClob)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(String columnLabel, NClob nClob)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(int columnIndex, Reader reader)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(String columnLabel, Reader reader)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(int columnIndex, Reader reader, long length)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNClob(String columnLabel, Reader reader, long length)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNString(int columnIndex, String nString)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateNString(String columnLabel, String nString)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateRowId(int columnIndex, RowId x) throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateRowId(String columnLabel, RowId x)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateSQLXML(int columnIndex, SQLXML xmlObject)
				throws SQLException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateSQLXML(String columnLabel, SQLXML xmlObject)
				throws SQLException {
			// TODO Auto-generated method stub
			
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

    private class XmlaOlap4jConnectionJdbc3 extends Olap4ldConnection {
        public XmlaOlap4jConnectionJdbc3(
            Olap4ldDriver driver,
            XmlaOlap4jProxy proxy,
            String url,
            Properties info)
            throws SQLException
        {
            super(FactoryJdbc3Impl.this, driver, proxy, url, info);
        }

		@Override
		public Array createArrayOf(String arg0, Object[] arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Blob createBlob() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Clob createClob() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NClob createNClob() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLXML createSQLXML() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Struct createStruct(String arg0, Object[] arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Properties getClientInfo() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getClientInfo(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isValid(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setClientInfo(Properties arg0)
				throws SQLClientInfoException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setClientInfo(String arg0, String arg1)
				throws SQLClientInfoException {
			// TODO Auto-generated method stub
			
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

    private static class XmlaOlap4jDatabaseMetaDataJdbc3
        extends Olap4ldDatabaseMetaData
    {
        public XmlaOlap4jDatabaseMetaDataJdbc3(
            Olap4ldConnection olap4jConnection)
        {
            super(olap4jConnection);
        }

		@Override
		public boolean autoCommitFailureClosesAllResultSets()
				throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public ResultSet getClientInfoProperties() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ResultSet getFunctionColumns(String arg0, String arg1,
				String arg2, String arg3) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ResultSet getFunctions(String arg0, String arg1, String arg2)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RowIdLifetime getRowIdLifetime() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ResultSet getSchemas(String arg0, String arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean supportsStoredFunctionsUsingCallSyntax()
				throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean generatedKeyAlwaysReturned() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public ResultSet getPseudoColumns(String catalog, String schemaPattern,
				String tableNamePattern, String columnNamePattern)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}
    }
}

// End FactoryJdbc3Impl.java
