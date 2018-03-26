// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
