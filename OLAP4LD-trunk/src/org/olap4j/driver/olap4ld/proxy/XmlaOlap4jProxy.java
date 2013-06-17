/*
// $Id: XmlaOlap4jProxy.java 435 2011-04-08 17:47:30Z lucboudreau $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package org.olap4j.driver.olap4ld.proxy;

import java.util.concurrent.Future;

import org.olap4j.driver.olap4ld.Olap4ldServerInfos;
import org.olap4j.driver.olap4ld.proxy.XmlaOlap4jProxyException;

/**
 * Defines a common set of methods for proxy objects.
 * @version $Id: XmlaOlap4jProxy.java 435 2011-04-08 17:47:30Z lucboudreau $
 */
public interface XmlaOlap4jProxy {
    /**
     * Sends a request to a URL and returns the response.
     *
     * @param serverInfos Server infos.
     * @param request Request string
     * @return Response The byte array that contains the whole response
     * from the server.
     * @throws XmlaOlap4jProxyException If anything occurs during the
     * request execution.
     */
    byte[] get(
        Olap4ldServerInfos serverInfos,
        String request)
            throws XmlaOlap4jProxyException;

    /**
     * Submits a request for background execution.
     *
     * @param serverInfos Server infos.
     * @param request Request
     * @return Future object representing the submitted job
     */
    Future<byte[]> submit(
        Olap4ldServerInfos serverInfos,
        String request);

    /**
     * Returns the name of the character set use for encoding the XML
     * string.
     */
    String getEncodingCharsetName();
}

// End XmlaOlap4jProxy.java
