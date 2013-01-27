/*
// $Id: XmlaOlap4jProxyException.java 315 2010-05-29 00:56:11Z jhyde $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.ld.proxy;

/**
 * Gets thrown whenever an exception is encountered during the querying
 * of an XmlaOlap4jProxy subclass.
 *
 * @author Luc Boudreau
 * @version $Id: XmlaOlap4jProxyException.java 315 2010-05-29 00:56:11Z jhyde $
 */
public class XmlaOlap4jProxyException extends Exception {
    private static final long serialVersionUID = 1729906649527317997L;
    public XmlaOlap4jProxyException(String message, Throwable cause) {
        super(message, cause);
    }
}

// End XmlaOlap4jProxyException.java