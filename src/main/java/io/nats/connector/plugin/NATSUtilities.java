/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.connector.plugin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.URL;

import java.io.IOException;

/**
 * Utilities used by the plugin.
 */
public class NATSUtilities {

    /**
     * Reads a url and returns the content as a string.  Useful for reading a
     * configuration in a cloud environment.
     *
     * @param url url to read from
     * @return url contents as a string
     * @throws IOException The uri cannot be read.
     */
    public static String readFromUrl(String url) throws IOException
    {
        InputStream is = new URL(url).openStream();

        StringBuilder sb = null;

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

            String line;
            while ((line = br.readLine()) != null)
            {
                if (sb == null)
                    sb = new StringBuilder();

                sb.append(line);
            }

            return sb.toString();

        } finally {
            is.close();
        }
    }
}
