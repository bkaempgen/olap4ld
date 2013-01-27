/*
//
// Licensed to Benedikt Kämpgen under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Benedikt Kämpgen licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.olap4j.driver.ld;

import org.olap4j.*;
import org.olap4j.driver.ld.LdOlap4jCellSet;
import org.olap4j.impl.UnmodifiableArrayMap;
import org.olap4j.metadata.Property;

import java.sql.ResultSet;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.Cell}
 * for XML/A providers.
 *
 * @author jhyde
 * @version $Id: XmlaOlap4jCell.java 404 2011-03-18 21:54:56Z lucboudreau $
 * @since Dec 5, 2007
 */
class LdOlap4jCell implements Cell {
    private final LdOlap4jCellSet cellSet;
    private final int ordinal;
    private final Object value;
    private final String formattedValue;
    private final Map<Property, Object> propertyValues;

    LdOlap4jCell(
        LdOlap4jCellSet cellSet,
        int ordinal,
        Object value,
        String formattedValue,
        Map<Property, Object> propertyValues)
    {
        this.cellSet = cellSet;
        this.ordinal = ordinal;
        this.value = value;
        this.formattedValue = formattedValue;

        // Use an ArrayMap for memory efficiency, because cells
        // typically have few properties, but there are a lot of cells
        this.propertyValues = UnmodifiableArrayMap.of(propertyValues);
    }

    public CellSet getCellSet() {
        return cellSet;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public List<Integer> getCoordinateList() {
        return cellSet.ordinalToCoordinates(ordinal);
    }

    public Object getPropertyValue(Property property) {
        return propertyValues.get(property);
    }

    public boolean isEmpty() {
        // FIXME
        return isNull();
    }

    public boolean isError() {
        return false;
    }

    public boolean isNull() {
        return value == null;
    }

    public double getDoubleValue() throws OlapException {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value.equals("") || value == null) {
        	return 0;
        } else {
            return Double.valueOf(String.valueOf(value));
        }
    }

    public String getErrorText() {
        return null; // FIXME:
    }

    public Object getValue() {
        return value;
    }

    public String getFormattedValue() {
        return formattedValue;
    }

    public ResultSet drillThrough() throws OlapException {
        throw new UnsupportedOperationException();
    }

    public void setValue(
        Object value,
        AllocationPolicy allocationPolicy,
        Object... allocationArgs)
    {
        throw new UnsupportedOperationException();
    }
}

// End XmlaOlap4jCell.java
