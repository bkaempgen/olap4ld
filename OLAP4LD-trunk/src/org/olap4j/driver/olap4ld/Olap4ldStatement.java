/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.olap4ld;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.olap4j.AllocationPolicy;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.CellSetListener;
import org.olap4j.CellSetMetaData;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.driver.olap4ld.helper.LdHelper;
import org.olap4j.mdx.AxisNode;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeWriter;
import org.olap4j.mdx.SelectNode;
import org.olap4j.mdx.parser.MdxParser;
import org.olap4j.mdx.parser.MdxParserFactory;
import org.olap4j.mdx.parser.MdxValidator;
import org.olap4j.metadata.Property;

/**
 * Implementation of {@link org.olap4j.OlapStatement} for XML/A providers.
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jStatement.java 427 2011-03-24 04:53:53Z lucboudreau $
 * @since May 24, 2007
 */
class Olap4ldStatement implements OlapStatement {
	final Olap4ldConnection olap4jConnection;
	private boolean closed;

	/**
	 * Current cell set, or null if the statement is not executing anything. Any
	 * method which modifies this member must synchronize on the
	 * {@link Olap4ldStatement}.
	 */
	Olap4ldCellSet openCellSet;
	private boolean canceled;
	int timeoutSeconds;
	Future<byte[]> future;

	/**
	 * Creates an XmlaOlap4jStatement.
	 * 
	 * @param olap4jConnection
	 *            Connection
	 */
	Olap4ldStatement(Olap4ldConnection olap4jConnection) {
		assert olap4jConnection != null;
		this.olap4jConnection = olap4jConnection;
		this.closed = false;
	}

	/**
	 * Returns the error-handler.
	 * 
	 * @return Error handler
	 */
	private LdHelper getHelper() {
		return olap4jConnection.helper;
	}

	// implement Statement

	public ResultSet executeQuery(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unused")
	private void checkOpen() throws SQLException {
		if (closed) {
			throw getHelper().createException("closed");
		}
	}

	public int executeUpdate(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void close() throws SQLException {
		if (!closed) {
			closed = true;
			if (openCellSet != null) {
				CellSet c = openCellSet;
				openCellSet = null;
				c.close();
			}
		}
	}

	public int getMaxFieldSize() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setMaxFieldSize(int max) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getMaxRows() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setMaxRows(int max) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getQueryTimeout() throws SQLException {
		return timeoutSeconds;
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		if (seconds < 0) {
			throw getHelper().createException(
					"illegal timeout value " + seconds);
		}
		this.timeoutSeconds = seconds;
	}

	public synchronized void cancel() {
		if (!canceled) {
			canceled = true;
			if (future != null) {
				future.cancel(true);
			}
		}
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void clearWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setCursorName(String name) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean execute(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getResultSet() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getUpdateCount() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean getMoreResults() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setFetchDirection(int direction) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getFetchDirection() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setFetchSize(int rows) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getFetchSize() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getResultSetConcurrency() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getResultSetType() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void addBatch(String sql) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void clearBatch() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int[] executeBatch() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public OlapConnection getConnection() {
		return olap4jConnection;
	}

	public boolean getMoreResults(int current) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int executeUpdate(String sql, int columnIndexes[])
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int executeUpdate(String sql, String columnNames[])
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean execute(String sql, int columnIndexes[]) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean execute(String sql, String columnNames[])
			throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getResultSetHoldability() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isPoolable() throws SQLException {
		throw new UnsupportedOperationException();
	}

	// implement Wrapper

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this)) {
			return iface.cast(this);
		}
		throw getHelper().createException("does not implement '" + iface + "'");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	// implement OlapStatement

	public CellSet executeOlapQuery(String mdx) throws OlapException {

		// This mdx string includes possible comments
		Olap4ldUtil._log.info("Parse MDX: " + mdx);
		long time = System.currentTimeMillis();

		// TODO: Added in order to generically parse mdx and use for querying
		// with SPARQL
		MdxParserFactory parserFactory = olap4jConnection.getParserFactory();
		MdxParser parser = parserFactory.createMdxParser(olap4jConnection);
		SelectNode select = parser.parseSelect(mdx);
		MdxValidator validator = parserFactory
				.createMdxValidator(olap4jConnection);
		select = validator.validateSelect(select);
		
		time = System.currentTimeMillis() - time;
		Olap4ldUtil._log.info("Parse MDX: finished in " + time + "ms.");
		
		CellSet cellset = executeOlapQuery(select);
		
		return cellset;

	}

	/**
	 * 
	 * 1) Creates and fills cellSetMetadata 2) Creates and fills cellSet.
	 * 
	 * 
	 * @param selectNode
	 * @return
	 * @throws OlapException
	 */
	public CellSet executeOlapQuery(SelectNode selectNode) throws OlapException {

		// Close the previous open CellSet, if there is one.
		synchronized (this) {
			if (openCellSet != null) {
				final Olap4ldCellSet cs = openCellSet;
				openCellSet = null;
				try {
					cs.close();
				} catch (SQLException e) {
					throw getHelper().createException(
							"Error while closing previous CellSet", e);
				}
			}

			// this.future =
			// olap4jConnection.proxy.submit(
			// olap4jConnection.serverInfos, mdx);
			openCellSet = olap4jConnection.factory.newCellSet(this);
		}
		
		final String mdx = toString(selectNode);
		
		Olap4ldUtil._log.info("Transform MDX parse tree: " + mdx);
		long time = System.currentTimeMillis();
		
		// We actually only need to populate the cellset once we start asking for content, right?
		openCellSet.createMetadata(selectNode);
		
		time = System.currentTimeMillis() - time;
		Olap4ldUtil._log.info("Transform MDX parse tree: finished in " + time + "ms.");
		
		// Implement NonEmpty
		/*
		 * The NonEmptyResult is more advanced since in theory it would be
		 * possible to have arbitrarily many axes It did not work properly yet,
		 * since we haven't implemented getCell, yet. Could be more performant
		 * than SimpleNonEmptyResult.
		 */
		//
		// for (int i = 0; i < selectNode.getAxisList().size(); i++) {
		// AxisNode axis = selectNode.getAxisList().get(i);
		// if (axis.isNonEmpty()) {
		// try {
		// openCellSet = new NonEmptyResult(openCellSet, selectNode, i);
		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// }

		CellSet nonEmptyCellSet = null;
		try {
			nonEmptyCellSet = (CellSet) new SimpleNonEmptyResult(openCellSet,
					selectNode);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return nonEmptyCellSet;
	}



	public void addListener(CellSetListener.Granularity granularity,
			CellSetListener listener) throws OlapException {
		throw getHelper().createException(
				"This driver does not support the cell listener API.");
	}

	/**
	 * Waits for an XMLA request to complete.
	 * 
	 * <p>
	 * You must not hold the monitor on this Statement when calling this method;
	 * otherwise {@link #cancel()} will not be able to operate.
	 * 
	 * @return Byte array resulting from successful request
	 * 
	 * @throws OlapException
	 *             if error occurred, or request timed out or was canceled
	 */
	byte[] getBytes() throws OlapException {
		synchronized (this) {
			if (future == null) {
				throw new IllegalArgumentException();
			}
		}
		try {
			// Wait for the request to complete, with timeout if necessary.
			// Whether or not timeout is used, the request can still be
			// canceled.
			if (timeoutSeconds > 0) {
				return future.get(timeoutSeconds, TimeUnit.SECONDS);
			} else {
				return future.get();
			}
		} catch (InterruptedException e) {
			throw getHelper().createException(null, e);
		} catch (ExecutionException e) {
			throw getHelper().createException(null, e.getCause());
		} catch (TimeoutException e) {
			throw getHelper().createException(
					"Query timeout of " + timeoutSeconds + " seconds exceeded");
		} catch (CancellationException e) {
			throw getHelper().createException("Query canceled");
		} finally {
			synchronized (this) {
				if (future == null) {
					throw new IllegalArgumentException();
				}
				future = null;
			}
		}
	}

	/**
	 * Converts a {@link org.olap4j.mdx.ParseTreeNode} to MDX string.
	 * 
	 * @param node
	 *            Parse tree node
	 * @return MDX text
	 */
	private static String toString(ParseTreeNode node) {
		StringWriter sw = new StringWriter();
		ParseTreeWriter parseTreeWriter = new ParseTreeWriter(sw);
		node.unparse(parseTreeWriter);
		return sw.toString();
	}

	/**
	 * Simple implementation of non-empty clause. We simply wrap the result
	 * object and create new axes with reduced positions.
	 * 
	 * The getCell methods need to be implemented.
	 * 
	 * @author benedikt
	 * 
	 */
	static class SimpleNonEmptyResult implements CellSet {

		private ArrayList<CellSetAxis> mutableAxesList = new ArrayList<CellSetAxis>();
		final CellSet underlying;
		/**
		 * Maps for offset from non-empty to underlying cellset.
		 */
		private final Map<Integer, Integer> columnN2UMap = new HashMap<Integer, Integer>();
		private final Map<Integer, Integer> rowN2UMap = new HashMap<Integer, Integer>();
		private final Map<Integer, Integer> columnU2NMap = new HashMap<Integer, Integer>();
		private final Map<Integer, Integer> rowU2NMap = new HashMap<Integer, Integer>();

		SimpleNonEmptyResult(CellSet result, SelectNode selectNode)
				throws SQLException {
			// copy of all axes
			// super(query, result.getAxes().clone());
			super();
			// copy
			List<CellSetAxis> immutableList = result.getAxes();
			for (CellSetAxis cellSetAxis : immutableList) {
				this.mutableAxesList.add(cellSetAxis);
			}

			underlying = result;

			// TODO: Does not wresultork, yet.
			// Now, I consider NON EMPTY
			// for each axis I ask whether the non-empty flag is set.
			AxisNode columnAxisNode = selectNode.getAxisList().get(0);
			Olap4ldCellSetAxis columnAxis = (Olap4ldCellSetAxis) mutableAxesList
					.get(0);
			List<Position> columnPositions = columnAxis.getPositions();
			List<Position> newColumnPositions = new ArrayList<Position>();

			AxisNode rowAxisNode = selectNode.getAxisList().get(1);
			Olap4ldCellSetAxis rowAxis = (Olap4ldCellSetAxis) mutableAxesList
					.get(1);
			List<Position> rowPositions = rowAxis.getPositions();
			List<Position> newRowPositions = new ArrayList<Position>();

			/* 
			 * TODO: Problem: The entire query has to be issued since we need to find empty rows
			 * and columns.
			 */
			
			if (columnAxisNode.isNonEmpty()) {

				// I take one position of that axis.
				// I go through all position of the other axes
				// If always null, we remove position of that axis.
				int i = -1;
				for (Position columnPosition : columnPositions) {
					i++;
					boolean remove = true;
					for (Position rowPosition : rowPositions) {
						if (underlying.getCell(columnPosition, rowPosition) == null
								|| underlying.getCell(columnPosition,
										rowPosition).isEmpty()
								|| underlying
										.getCell(columnPosition, rowPosition)
										.getFormattedValue().equals("")) {
							;
						} else {
							remove = false;
							break;
						}
					}
					if (!remove) {
						newColumnPositions.add(columnPosition);
						/*
						 * Now, we should have an external pos mapped to an
						 * internal/underlying pos
						 * 
						 * It is enough to have that on a per-axis base. row i
						 * => j, column e => f
						 * 
						 * Any
						 */
						this.columnN2UMap.put(newColumnPositions.size() - 1, i);
						this.columnU2NMap.put(i, newColumnPositions.size() - 1);
					}
				}
				mutableAxesList.set(0, new Olap4ldCellSetAxis(
						(Olap4ldCellSet) result, mutableAxesList.get(0)
								.getAxisOrdinal(), newColumnPositions));
			}

			if (rowAxisNode.isNonEmpty()) {

				// I take one position of that axis.
				// I go through all position of the other axes
				// If always null, we remove position of that axis.
				int i = -1;
				for (Position rowPosition : rowPositions) {
					i++;
					boolean remove = true;
					for (Position columnPosition : columnPositions) {
						if (underlying.getCell(columnPosition, rowPosition) == null
								|| underlying.getCell(columnPosition,
										rowPosition).isEmpty()
								|| underlying
										.getCell(columnPosition, rowPosition)
										.getFormattedValue().equals("")) {
							;
						} else {
							remove = false;
							break;
						}
					}
					if (!remove) {
						newRowPositions.add(rowPosition);
						/*
						 * Now, we should have an external pos mapped to an
						 * internal/underlying pos
						 * 
						 * It is enough to have that on a per-axis base. row i
						 * => j, column e => f
						 * 
						 * Any
						 */
						this.rowN2UMap.put(newRowPositions.size() - 1, i);
						this.rowU2NMap.put(i, newRowPositions.size() - 1);
					}
				}

				mutableAxesList.set(1, new Olap4ldCellSetAxis(
						(Olap4ldCellSet) result, mutableAxesList.get(1)
								.getAxisOrdinal(), newRowPositions));
			}

		}

		/**
		 * Get non-empty axes.
		 */
		public List<CellSetAxis> getAxes() {
			return this.mutableAxesList;
		}

		/**
		 * 
		 * Here, we query for non-empty coordinates, which we need to map to
		 * underlying coordinates.
		 * 
		 */
		public Cell getCell(List<Integer> coordinates) {
			// It could be that column row maps are empty
			if (this.columnN2UMap.isEmpty() || this.rowN2UMap.isEmpty()) {
				return underlying.getCell(coordinates);
			}

			/*
			 * I have coordinates for column and rows and have an offset for
			 * both in map.
			 */
			List<Integer> newCoordinates = new ArrayList<Integer>();
			newCoordinates.add(this.columnN2UMap.get(coordinates.get(0))); // Column
																			// offset
			newCoordinates.add(this.rowN2UMap.get(coordinates.get(1))); // Row
																		// offset

			Cell cell = underlying.getCell(newCoordinates);

			return new NonEmptyCell(cell, this);
		}

		/**
		 * Here, we get underlying ordinal, since we only have original ordinals
		 * (non-empty are underlying). We can directly query the underlying for
		 * the cell.
		 */
		public Cell getCell(int ordinal) {
			Cell cell = underlying.getCell(ordinal);
			return new NonEmptyCell(cell, this);
		}

		/**
		 * Here, we get underlying positions, since positions do not change. We
		 * get their ordinals, which we use directly to ask underlying for the
		 * value.
		 */
		public Cell getCell(Position... positions) {
			if (positions.length != underlying.getAxes().size()) {
				throw new IllegalArgumentException(
						"cell coordinates should have dimension "
								+ underlying.getAxes().size());
			}
			List<Integer> coords = new ArrayList<Integer>(positions.length);
			for (Position position : positions) {
				coords.add(position.getOrdinal());
			}
			Cell cell = underlying.getCell(coords);
			return new NonEmptyCell(cell, this);
		}

		@Override
		public OlapStatement getStatement() throws SQLException {
			return underlying.getStatement();
		}

		@Override
		public CellSetAxis getFilterAxis() {
			return underlying.getFilterAxis();
		}

		/**
		 * Here, we get as input an ordinal from underlying, since all cells and
		 * positions have their original ordinals. Those ordinals we convert to
		 * underlying coordinates.
		 * 
		 * Those underlying coordinates we can transform to non-empty
		 * coordinates.
		 * 
		 */
		public List<Integer> ordinalToCoordinates(int ordinal) {
			List<Integer> underlyingCoordinateList = underlying
					.ordinalToCoordinates(ordinal);

			// It could be that column row maps are empty
			if (this.columnU2NMap.isEmpty() || this.rowU2NMap.isEmpty()) {
				return underlyingCoordinateList;
			}

			List<Integer> newCoordinates = new ArrayList<Integer>();
			newCoordinates.add(this.columnU2NMap.get(underlyingCoordinateList
					.get(0))); // Column offset
			newCoordinates.add(this.rowU2NMap.get(underlyingCoordinateList
					.get(1))); // Row offset

			return newCoordinates;
		}

		/**
		 * Here, we get non-empty coordinates, which we first need to translate
		 * to underlying coordinates and which we then can transform to
		 * underlying ordinals. Those ordinals are non-empty ordinals, already.
		 */
		public int coordinatesToOrdinal(List<Integer> coordinates) {
			// Could be that maps are empty.
			if (columnN2UMap.isEmpty() || rowN2UMap.isEmpty()) {
				return underlying.coordinatesToOrdinal(coordinates);
			}

			List<Integer> newCoordinates = new ArrayList<Integer>();
			newCoordinates.add(this.columnN2UMap.get(coordinates.get(0))); // Column
																			// offset
			newCoordinates.add(this.rowN2UMap.get(coordinates.get(1))); // Row
																		// offset

			return underlying.coordinatesToOrdinal(newCoordinates);
		}

		@Override
		public CellSetMetaData getMetaData() {
			try {
				return underlying.getMetaData();
			} catch (OlapException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
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
		public boolean absolute(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void afterLast() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void beforeFirst() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void cancelRowUpdates() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void clearWarnings() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void deleteRow() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public int findColumn(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean first() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Array getArray(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Array getArray(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InputStream getAsciiStream(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InputStream getAsciiStream(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(String arg0, int arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InputStream getBinaryStream(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InputStream getBinaryStream(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Blob getBlob(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Blob getBlob(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean getBoolean(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean getBoolean(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public byte getByte(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public byte getByte(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public byte[] getBytes(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public byte[] getBytes(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Reader getCharacterStream(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Reader getCharacterStream(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Clob getClob(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Clob getClob(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getConcurrency() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getCursorName() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Date getDate(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Date getDate(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Date getDate(int arg0, Calendar arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Date getDate(String arg0, Calendar arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public double getDouble(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public double getDouble(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getFetchDirection() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getFetchSize() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float getFloat(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float getFloat(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getInt(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getInt(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getLong(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getLong(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object getObject(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object getObject(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object getObject(int arg0, Map<String, Class<?>> arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object getObject(String arg0, Map<String, Class<?>> arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Ref getRef(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Ref getRef(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getRow() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public short getShort(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public short getShort(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getString(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getString(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Time getTime(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Time getTime(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Time getTime(int arg0, Calendar arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Time getTime(String arg0, Calendar arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Timestamp getTimestamp(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Timestamp getTimestamp(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Timestamp getTimestamp(int arg0, Calendar arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Timestamp getTimestamp(String arg0, Calendar arg1)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getType() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public URL getURL(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public URL getURL(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InputStream getUnicodeStream(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InputStream getUnicodeStream(String arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void insertRow() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean isAfterLast() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isBeforeFirst() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isFirst() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isLast() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean last() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void moveToCurrentRow() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void moveToInsertRow() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean next() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean previous() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void refreshRow() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean relative(int arg0) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean rowDeleted() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean rowInserted() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean rowUpdated() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setFetchDirection(int arg0) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void setFetchSize(int arg0) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateArray(int arg0, Array arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateArray(String arg0, Array arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateAsciiStream(int arg0, InputStream arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateAsciiStream(String arg0, InputStream arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBigDecimal(int arg0, BigDecimal arg1)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBigDecimal(String arg0, BigDecimal arg1)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBinaryStream(int arg0, InputStream arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBinaryStream(String arg0, InputStream arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBlob(int arg0, Blob arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBlob(String arg0, Blob arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBoolean(int arg0, boolean arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBoolean(String arg0, boolean arg1)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateByte(int arg0, byte arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateByte(String arg0, byte arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBytes(int arg0, byte[] arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateBytes(String arg0, byte[] arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateCharacterStream(int arg0, Reader arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateCharacterStream(String arg0, Reader arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateClob(int arg0, Clob arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateClob(String arg0, Clob arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateDate(int arg0, Date arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateDate(String arg0, Date arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateDouble(int arg0, double arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateDouble(String arg0, double arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateFloat(int arg0, float arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateFloat(String arg0, float arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateInt(int arg0, int arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateInt(String arg0, int arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateLong(int arg0, long arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateLong(String arg0, long arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateNull(int arg0) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateNull(String arg0) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateObject(int arg0, Object arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateObject(String arg0, Object arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateObject(int arg0, Object arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateObject(String arg0, Object arg1, int arg2)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateRef(int arg0, Ref arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateRef(String arg0, Ref arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateRow() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateShort(int arg0, short arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateShort(String arg0, short arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateString(int arg0, String arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateString(String arg0, String arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateTime(int arg0, Time arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateTime(String arg0, Time arg1) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateTimestamp(int arg0, Timestamp arg1)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void updateTimestamp(String arg0, Timestamp arg1)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean wasNull() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isWrapperFor(Class<?> arg0) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public <T> T unwrap(Class<T> arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

	}

	/**
	 * Class to represent a non-empty cell. Could be empty, though. Introduced,
	 * since a normal cell would ask its cellset, and not the non-empty cell set
	 * for its coordinates.
	 * 
	 * @author benedikt
	 * 
	 */
	static class NonEmptyCell implements Cell {

		private Cell underlying;
		private CellSet underlyingCellSet;

		public NonEmptyCell(Cell cell, CellSet cellset) {
			this.underlying = cell;
			this.underlyingCellSet = cellset;
		}

		@Override
		public CellSet getCellSet() {
			// TODO Auto-generated method stub
			return underlyingCellSet;
		}

		@Override
		public int getOrdinal() {
			return underlying.getOrdinal();
		}

		/**
		 * Coordingates should be dependent on the underlyingcellset.
		 */
		public List<Integer> getCoordinateList() {
			return underlyingCellSet.ordinalToCoordinates(getOrdinal());
		}

		@Override
		public Object getPropertyValue(Property property) {
			return underlying.getPropertyValue(property);
		}

		@Override
		public boolean isEmpty() {
			return underlying.isEmpty();
		}

		@Override
		public boolean isError() {
			return underlying.isError();
		}

		@Override
		public boolean isNull() {
			return underlying.isNull();
		}

		@Override
		public double getDoubleValue() throws OlapException {
			return underlying.getDoubleValue();
		}

		@Override
		public String getErrorText() {
			return underlying.getErrorText();
		}

		@Override
		public Object getValue() {
			return underlying.getValue();
		}

		@Override
		public String getFormattedValue() {
			return underlying.getFormattedValue();
		}

		@Override
		public ResultSet drillThrough() throws OlapException {
			return underlying.drillThrough();
		}

		@Override
		public void setValue(Object value, AllocationPolicy allocationPolicy,
				Object... allocationArgs) throws OlapException {
			underlying.setValue(value, allocationPolicy, allocationArgs);
		}

	}

	/**
	 * A <code>NonEmptyResult</code> filters a result by removing empty rows on
	 * a particular axis.
	 * 
	 * This class is more advanced since it works axis-wise and could possibly
	 * filter arbitrarily many axes.
	 */
	static class NonEmptyResult extends Olap4ldCellSet {

		final CellSet underlying;
		private final int axis;
		private final Map<Integer, Integer> map;
		/** workspace. Synchronized access only. */
		private final int[] pos;
		// private CellSetAxis slicerAxis;
		private List<CellSetAxis> mutableAxesList;

		/**
		 * Creates a NonEmptyResult.
		 * 
		 * @param result
		 *            Result set
		 * @param query
		 *            Query
		 * @param axis
		 *            Which axis to make non-empty
		 * @throws SQLException
		 */
		NonEmptyResult(CellSet result, SelectNode query, int axis)
				throws SQLException {
			// copy of all axes
			// super(query, result.getAxes().clone());
			super((Olap4ldStatement) result.getStatement());
			// copy
			mutableAxesList = new ArrayList<CellSetAxis>();
			List<CellSetAxis> immutableList = result.getAxes();
			for (CellSetAxis cellSetAxis : immutableList) {
				this.mutableAxesList.add(cellSetAxis);
			}

			this.underlying = result;
			this.axis = axis;
			this.map = new HashMap<Integer, Integer>();
			int axisCount = underlying.getAxes().size();
			this.pos = new int[axisCount];
			// this.slicerAxis = underlying.getFilterAxis();
			List<Position> positions = underlying.getAxes().get(axis)
					.getPositions();

			final List<Position> positionsList;

			positionsList = new ArrayList<Position>();
			int i = -1;
			for (Position position : positions) {
				++i;
				if (!isEmpty(i, axis)) {
					map.put(positionsList.size(), i);
					positionsList.add(position);
				}
			}

			// Assuming that getAxes is called, we need to create a new axes
			// list
			this.mutableAxesList.set(axis, new Olap4ldCellSetAxis(
					(Olap4ldCellSet) result, mutableAxesList.get(axis)
							.getAxisOrdinal(), positionsList));
			// this.axes[axis] = new RolapAxis.PositionList(positionsList);
		}

		public List<CellSetAxis> getAxes() {
			return this.mutableAxesList;
		}

		public Cell getCell(List<Integer> coordinates) {
			return underlying.getCell(coordinates);
		}

		public Cell getCell(int ordinal) {
			return underlying.getCell(ordinal);
		}

		public Cell getCell(Position... positions) {
			return underlying.getCell(positions);
		}

		/**
		 * Returns true if all cells at a given offset on a given axis are
		 * empty. For example, in a 2x2x2 dataset, <code>isEmpty(1,0)</code>
		 * returns true if cells <code>{(1,0,0), (1,0,1), (1,1,0),
		 * (1,1,1)}</code> are all empty. As you can see, we hold the 0th
		 * coordinate fixed at 1, and vary all other coordinates over all
		 * possible values.
		 */
		private boolean isEmpty(int offset, int fixedAxis) {
			int axisCount = getAxes().size();
			pos[fixedAxis] = offset;
			return isEmptyRecurse(fixedAxis, axisCount - 1);
		}

		private boolean isEmptyRecurse(int fixedAxis, int axis) {
			if (axis < 0) {

				Cell cell = underlying.getCell(array2List(pos));
				return (cell.isNull() || cell.getFormattedValue().equals(""));
			} else if (axis == fixedAxis) {
				return isEmptyRecurse(fixedAxis, axis - 1);
			} else {
				List<Position> positions = getAxes().get(axis).getPositions();
				final int positionCount = positions.size();
				for (int i = 0; i < positionCount; i++) {
					pos[axis] = i;
					if (!isEmptyRecurse(fixedAxis, axis - 1)) {
						return false;
					}
				}
				return true;
			}
		}

		// synchronized because we use 'pos'
		public synchronized Cell getCell(int[] externalPos) {
			try {
				System.arraycopy(externalPos, 0, this.pos, 0,
						externalPos.length);
				int offset = (Integer) externalPos[axis];
				int mappedOffset = mapOffsetToUnderlying(offset);
				this.pos[axis] = mappedOffset;
				return underlying.getCell(array2List(this.pos));
			} catch (NullPointerException npe) {
				return underlying.getCell(array2List(externalPos));
			}
		}

		private List<Integer> array2List(int[] externalPos) {
			List<Integer> externalPosList = new ArrayList<Integer>();
			for (int i = 0; i < externalPos.length; i++) {
				externalPosList.add((Integer) externalPos[i]);
			}
			return externalPosList;
		}

		private int mapOffsetToUnderlying(int offset) {
			return map.get(offset);
		}

		public void close() {
			try {
				underlying.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
}

// End XmlaOlap4jStatement.java
