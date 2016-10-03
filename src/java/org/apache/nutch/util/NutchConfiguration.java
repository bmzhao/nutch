/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to create Hadoop {@link Configuration}s that include Nutch-specific
 * resources.
 */
public class NutchConfiguration {
    public static Logger LOG = LoggerFactory
            .getLogger(NutchConfiguration.class);

    public static final String UUID_KEY = "nutch.conf.uuid";
    public static final String ALTERNATE_CONF_PATH = "nutch.conf.alternate.path";
    public static final String ALTERNATE_CONF_FS = "nutch.conf.alternate.fs";

    private NutchConfiguration() {
    } // singleton

    /*
     * Configuration.hashCode() doesn't return values that correspond to a unique
     * set of parameters. This is a workaround so that we can track instances of
     * Configuration created by Nutch.
     */
    private static void setUUID(Configuration conf) {
        UUID uuid = UUID.randomUUID();
        conf.set(UUID_KEY, uuid.toString());
    }

    /**
     * Retrieve a Nutch UUID of this configuration object, or null if the
     * configuration was created elsewhere.
     *
     * @param conf
     *          configuration instance
     * @return uuid or null
     */
    public static String getUUID(Configuration conf) {
        return conf.get(UUID_KEY);
    }

    /**
     * Create a {@link Configuration} for Nutch. This will load the standard Nutch
     * resources, <code>nutch-default.xml</code> and <code>nutch-site.xml</code>
     * overrides.
     */
    public static Configuration create() {
        Configuration conf = new Configuration();
        setUUID(conf);
        addNutchResources(conf);
        return conf;
    }

    /**
     * Create a {@link Configuration} from supplied properties.
     *
     * @param addNutchResources
     *          if true, then first <code>nutch-default.xml</code>, and then
     *          <code>nutch-site.xml</code> will be loaded prior to applying the
     *          properties. Otherwise these resources won't be used.
     * @param nutchProperties
     *          a set of properties to define (or override)
     */
    public static Configuration create(boolean addNutchResources,
                                       Properties nutchProperties) {
        Configuration conf = new Configuration();

        setUUID(conf);
        if (addNutchResources) {
            addNutchResources(conf);
        }
        for (Entry<Object, Object> e : nutchProperties.entrySet()) {
            conf.set(e.getKey().toString(), e.getValue().toString());
        }
        return conf;
    }

    /**
     * Add the standard Nutch resources to {@link Configuration}.
     *
     * @param conf
     *          Configuration object to which configuration is to be added.
     *
     * Reading config from hdfs
     * http://blog.rajeevsharma.in/2009/06/using-hdfs-in-java-0200.html
     */
    private static Configuration addNutchResources(Configuration conf) {
        conf.addResource("nutch-default.xml");
        conf.addResource("nutch-site.xml");

        String fs = conf.get(ALTERNATE_CONF_FS, "file");

        String alternateConfigPath = conf.get(ALTERNATE_CONF_PATH);
        if (!StringUtils.isBlank(alternateConfigPath)) {
            switch (fs) {
                case "file":
                    java.nio.file.Path alternateLocalFSPath = FileSystems.getDefault().getPath(alternateConfigPath);
                    if (Files.exists(alternateLocalFSPath)) {
                        try {
                            LOG.info("Loading alternate config from local fs path: {}", alternateLocalFSPath.toString());
                            conf.addResource(new FileInputStream(alternateLocalFSPath.toFile()));
                        } catch (FileNotFoundException e) {
                            LOG.error(ExceptionUtils.getStackTrace(e));
                        }
                    }
                    break;
                case "hdfs":
                    try {
                        FileSystem fileSystem = FileSystem.get(conf);
                        Path hdfsPath = new Path(alternateConfigPath);
                        if (fileSystem.exists(hdfsPath)) {
                            LOG.info("Loading alternate config from hdfs path: {}", hdfsPath.toString());
                            conf.addResource(fileSystem.open(hdfsPath));
                        }
                    } catch (IOException e) {
                        LOG.error(ExceptionUtils.getStackTrace(e));
                    }
                    break;
            }
        }

        return conf;
    }
}
