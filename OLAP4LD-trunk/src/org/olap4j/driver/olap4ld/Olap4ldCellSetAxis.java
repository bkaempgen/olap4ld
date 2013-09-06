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
package org.olap4j.driver.olap4ld;

import org.olap4j.*;
import org.olap4j.driver.olap4ld.Olap4ldCellSet;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.CellSetAxis}
 * for XML/A providers.
 *
 * @author jhyde, bkaempgen
 * @version $Id: XmlaOlap4jCellSetAxis.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 5, 2007
 */
class Olap4ldCellSetAxis implements CellSetAxis {
    private final Olap4ldCellSet olap4jCellSet;
    private final Axis axis;
    final List<Position> positions;

    /**
     * Creates an XmlaOlap4jCellSetAxis.
     *
     * @param olap4jCellSet Cell set
     * @param axis Axis identifier
     * @param positions List of positions. Caller must ensure it is immutable
     */
    public Olap4ldCellSetAxis(
        Olap4ldCellSet olap4jCellSet,
        Axis axis,
        List<Position> positions)
    {
        this.olap4jCellSet = olap4jCellSet;
        this.axis = axis;
        this.positions = positions;
    }

    public Axis getAxisOrdinal() {
        return axis;
    }

    public CellSet getCellSet() {
        return olap4jCellSet;
    }

    public CellSetAxisMetaData getAxisMetaData() {
        final CellSetMetaData cellSetMetaData = olap4jCellSet.getMetaData();
        if (axis.isFilter()) {
            return cellSetMetaData.getFilterAxisMetaData();
        } else {
            return cellSetMetaData.getAxesMetaData().get(
                axis.axisOrdinal());
        }
    }

    public List<Position> getPositions() {
        return positions;
    }

    public int getPositionCount() {
        return positions.size();
    }

    public ListIterator<Position> iterator() {
        return positions.listIterator();
    }
}

// End XmlaOlap4jCellSetAxis.java
