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

import org.olap4j.OlapException;
import org.olap4j.driver.olap4ld.Olap4ldConnection;
import org.olap4j.driver.olap4ld.Olap4ldConnection.MetadataRequest;
import org.olap4j.impl.Named;
import org.olap4j.impl.NamedListImpl;
import org.olap4j.metadata.NamedList;

import java.util.AbstractList;
import java.util.Map;

/**
 * Named list which instantiates itself on first use.
 * 
 * <p>
 * <code>DeferredNamedListImpl</code> is useful way to load an object model
 * representing a hierarchical schema. If a catalog contains schemas, which
 * contain cubes, which contain dimensions, and so forth, and if all collections
 * loaded immediately, loading the catalog would immediately load all
 * sub-objects into memory, taking a lot of memory and time.
 * 
 * <p>
 * This class is not gc-friendly at present. Once populated,
 * <code>DeferredNamedListImpl</code> holds hard references to the objects it
 * contains, so they are not available to be garbage-collected. Support for weak
 * references might be a future enhancement to this class.
 * </p>
 * 
 * @author jhyde, bkaempgen
 * @version $Id: DeferredNamedListImpl.java 470 2011-08-02 19:30:41Z jhyde $
 * @since Dec 4, 2007
 */
class DeferredNamedListImpl<T extends Named> extends AbstractList<T> implements
		NamedList<T> {
	private final NamedList<T> list = new NamedListImpl<T>();
	private State state = State.NEW;

	protected final Olap4ldConnection.MetadataRequest metadataRequest;
	protected final Olap4ldConnection.Context context;
	protected final Olap4ldConnection.Handler<T> handler;
	protected final Object[] restrictions;

	DeferredNamedListImpl(Olap4ldConnection.MetadataRequest metadataRequest,
			Olap4ldConnection.Context context,
			Olap4ldConnection.Handler<T> handler, Object[] restrictions) {
		this.metadataRequest = metadataRequest;
		this.context = context;
		this.handler = handler;
		this.restrictions = (restrictions == null) ? new Object[0]
				: restrictions;
	}

	/**
	 * Flushes the contents of the list. Next access will re-populate.
	 */
	void reset() {
		state = State.NEW;
		list.clear();
	}

	private NamedList<T> getList() {

		// No real solution, better is catch/try, but Saiku forces us
		while (state == State.POPULATING) {
			;
			System.out.println("still populating...");
		}

		switch (state) {
		case POPULATING:
			// return new NamedListImpl<T>();
			throw new RuntimeException("recursive population");
		case NEW:
			try {
				state = State.POPULATING;
				populateList(list);
				state = State.POPULATED;
			} catch (OlapException e) {
				state = State.NEW;
				// TODO: fetch metadata on getCollection() method, so we
				// can't get an exception while traversing the list
				throw new RuntimeException(e);
			}
			// fall through
		case POPULATED:
		default:
			return list;
		}
	}

	/**
	 * If someone is asking isEmpty
	 */
	public boolean isEmpty() {
		if (state == State.POPULATING) {
			return true;
		}
		return getList().isEmpty();
	}

	public T get(int index) {
		return getList().get(index);
	}

	public int size() {
		return getList().size();
	}

	/**
	 * If we want a specific element, we only ask for this specific element. No matter whether
	 * State.NEW or not.
	 */
	public T get(String name) {
		if (name != null) {
				
			try {
				// Create own restriction for cube
				if (metadataRequest == MetadataRequest.MDSCHEMA_CUBES) {
					Object[] restrictions = new Object[] { "CUBE_NAME", name };
					context.olap4jConnection.olap4jDatabaseMetaData
							.populateList(list, context, metadataRequest,
									handler, restrictions);
				}
				// TODO add other possible Requests.
				
				// Default
				getList();	
			} catch (OlapException e) {
				// Is a problem.
				throw new RuntimeException(e);
			}
		} else {
			getList();
		}
		return list.get(name);
	}

	public int indexOfName(String name) {
		return getList().indexOfName(name);
	}

	protected void populateList(NamedList<T> list) throws OlapException {
		context.olap4jConnection.olap4jDatabaseMetaData.populateList(list,
				context, metadataRequest, handler, restrictions);
	}

	private enum State {
		NEW, POPULATING, POPULATED
	}

	public String getName(Object element) {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<String, T> asMap() {
		// TODO Auto-generated method stub
		return null;
	}
}

// End DeferredNamedListImpl.java
