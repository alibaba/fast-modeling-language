/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.driver.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.aliyun.fastmodel.driver.cli.command.Console;
import picocli.CommandLine;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.PropertiesDefaultProvider;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * FastModel Main-Class
 *
 * @author panguanjing
 * @date 2020/12/22
 */
public class FastModel {

    public static final String FML_PROPERTIES = "fml_config.properties";
    public static final String JAR = ".jar";

    private FastModel() {

    }

    public static void main(String[] args) throws IOException {
        CommandLine commandLine = createCommandLine(new Console());
        System.exit(commandLine.execute(args));
    }

    public static CommandLine createCommandLine(Object command) throws IOException {
        CommandLine commandLine = new CommandLine(command);
        URL location = FastModel.class.getProtectionDomain().getCodeSource().getLocation();
        String file1 = location.getFile();
        String child = FML_PROPERTIES;
        File properties;
        if (file1.endsWith(JAR)) {
            File file = new File(file1);
            properties = new File(file.getParent(), child);
        } else {
            properties = new File(file1, child);
        }
        InputStreamReader inputStreamReader = new InputStreamReader(
            new FileInputStream(properties),
            StandardCharsets.UTF_8
        );
        Properties systemProperties = new Properties();
        systemProperties.load(inputStreamReader);
        commandLine.setDefaultValueProvider(new PropertiesDefaultProvider(systemProperties));
        return commandLine;
    }

    public static class VersionProvider
        implements IVersionProvider {
        @Override
        public String[] getVersion() {
            String version = getClass().getPackage().getImplementationVersion();
            return new String[] {"FastModel CLI " + firstNonNull(version, "(version unknown)")};
        }
    }

}
