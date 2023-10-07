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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.aliyun.fastmodel.driver.cli.FastModel.VersionProvider;
import com.aliyun.fastmodel.driver.cli.command.Console;
import org.junit.Test;
import picocli.CommandLine;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/22
 */
public class FastModelTest {

    @Test
    public void testCreateCommand() throws IOException {
        System.setIn(new ByteArrayInputStream("hello".getBytes()));
        System.setOut(new PrintStream(new ByteArrayOutputStream()));
        CommandLine commandLine = FastModel.createCommandLine(new Console());
        assertNotNull(commandLine);
    }

    @Test
    public void testGetVersion() {
        FastModel.VersionProvider versionProvider = new VersionProvider();
        String[] version = versionProvider.getVersion();
        String s = version[0];
        assertNotNull(s);
    }
}