/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld;

import org.olap4j.CellSetAxisMetaData;
import org.olap4j.CellSetMetaData;
import org.olap4j.driver.ld.LdOlap4jCellProperty;
import org.olap4j.driver.ld.LdOlap4jCellSetAxisMetaData;
import org.olap4j.driver.ld.LdOlap4jCellSetMetaData;
import org.olap4j.driver.ld.XmlaOlap4jPreparedStatement;
import org.olap4j.driver.ld.LdOlap4jStatement;
import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;

import java.sql.SQLException;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.CellSetMetaData}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jCellSetMetaData.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Jun 13, 2007
 */
class LdOlap4jCellSetMetaData implements CellSetMetaData {
    final LdOlap4jCube cube;
    private final NamedList<CellSetAxisMetaData> axisMetaDataList =
        new ArrayNamedListImpl<CellSetAxisMetaData>() {
            protected String getName(CellSetAxisMetaData axisMetaData) {
                return axisMetaData.getAxisOrdinal().name();
            }

			@Override
			public String getName(Object element) {
				// TODO Auto-generated method stub
				return null;
			}
        };
    private final LdOlap4jCellSetAxisMetaData filterAxisMetaData;
    private final NamedList<Property> cellProperties =
        new ArrayNamedListImpl<Property>() {
            protected String getName(Property property) {
                return property.getName();
            }

			@Override
			public String getName(Object element) {
				// TODO Auto-generated method stub
				return null;
			}
        };
    final Map<String, Property> propertiesByTag;

    LdOlap4jCellSetMetaData(
        LdOlap4jStatement olap4jStatement,
        LdOlap4jCube cube,
        LdOlap4jCellSetAxisMetaData filterAxisMetaData,
        List<LdOlap4jCellSetAxisMetaData> axisMetaDataList,
        List<LdOlap4jCellProperty> cellProperties)
    {
        assert olap4jStatement != null;
        assert cube != null;
        assert filterAxisMetaData != null;
        this.cube = cube;
        this.filterAxisMetaData = filterAxisMetaData;
        this.axisMetaDataList.addAll(axisMetaDataList);
        this.propertiesByTag = new HashMap<String, Property>();
        for (LdOlap4jCellProperty cellProperty : cellProperties) {
            Property property;
            try {
                property = Property.StandardCellProperty.valueOf(
                    cellProperty.propertyName);
                this.propertiesByTag.put(cellProperty.tag, property);
            } catch (IllegalArgumentException e) {
                property = cellProperty;
                this.propertiesByTag.put(property.getName(), property);
            }
            this.cellProperties.add(property);
        }
    }

    private LdOlap4jCellSetMetaData(
        LdOlap4jStatement olap4jStatement,
        LdOlap4jCube cube,
        LdOlap4jCellSetAxisMetaData filterAxisMetaData,
        List<CellSetAxisMetaData> axisMetaDataList,
        Map<String, Property> propertiesByTag,
        List<Property> cellProperties)
    {
        assert olap4jStatement != null;
        assert cube != null;
        assert filterAxisMetaData != null;
        this.cube = cube;
        this.filterAxisMetaData = filterAxisMetaData;
        this.axisMetaDataList.addAll(axisMetaDataList);
        this.propertiesByTag = propertiesByTag;
        this.cellProperties.addAll(cellProperties);
    }

    LdOlap4jCellSetMetaData cloneFor(
        XmlaOlap4jPreparedStatement preparedStatement)
    {
        return new LdOlap4jCellSetMetaData(
            preparedStatement,
            cube,
            filterAxisMetaData,
            axisMetaDataList,
            propertiesByTag,
            cellProperties);
    }

    // implement CellSetMetaData

    public NamedList<Property> getCellProperties() {
        return Olap4jUtil.cast(cellProperties);
    }

    public Cube getCube() {
        return cube;
    }

    public NamedList<CellSetAxisMetaData> getAxesMetaData() {
        return axisMetaDataList;
    }

    public CellSetAxisMetaData getFilterAxisMetaData() {
        return filterAxisMetaData;
    }

// implement ResultSetMetaData

    public int getColumnCount() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isSearchable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCurrency(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int isNullable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnLabel(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSchemaName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getPrecision(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getScale(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getTableName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCatalogName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getColumnType(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnTypeName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isReadOnly(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWritable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getColumnClassName(int column) throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}

// End XmlaOlap4jCellSetMetaData.java