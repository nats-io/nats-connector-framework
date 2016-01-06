// Copyright 2015 Apcera Inc.  All Rights Reserved.

package io.nats.connector.plugin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.URL;

import java.io.IOException;

/**
 * Created by colinsullivan on 1/4/16.
 */
public class NATSUtilities {

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
