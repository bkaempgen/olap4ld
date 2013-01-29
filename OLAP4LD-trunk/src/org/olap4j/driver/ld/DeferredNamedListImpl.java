/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.ld;

import org.olap4j.OlapException;
import org.olap4j.driver.ld.LdOlap4jConnection;
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
 * @author jhyde
 * @version $Id: DeferredNamedListImpl.java 470 2011-08-02 19:30:41Z jhyde $
 * @since Dec 4, 2007
 */
class DeferredNamedListImpl<T extends Named> extends AbstractList<T> implements
		NamedList<T> {
	private final NamedList<T> list = new NamedListImpl<T>();
	private State state = State.NEW;

	protected final LdOlap4jConnection.MetadataRequest metadataRequest;
	protected final LdOlap4jConnection.Context context;
	protected final LdOlap4jConnection.Handler<T> handler;
	protected final Object[] restrictions;

	DeferredNamedListImpl(LdOlap4jConnection.MetadataRequest metadataRequest,
			LdOlap4jConnection.Context context,
			LdOlap4jConnection.Handler<T> handler, Object[] restrictions) {
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
			//return new NamedListImpl<T>();
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

	public T get(String name) {
		return getList().get(name);
	}

	public int indexOfName(String name) {
		return getList().indexOfName(name);
	}

	protected void populateList(NamedList<T> list) throws OlapException {
		context.olap4jConnection.populateList(list, context, metadataRequest,
				handler, restrictions);
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
