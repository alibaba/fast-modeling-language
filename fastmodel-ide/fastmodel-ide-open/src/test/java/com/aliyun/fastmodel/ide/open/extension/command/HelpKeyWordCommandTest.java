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

package com.aliyun.fastmodel.ide.open.extension.command;

import java.util.Locale;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.HelpCommand;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.ResourceBundleMessageSource;

import static org.junit.Assert.assertEquals;

/**
 * define the help keyword command
 *
 * @author panguanjing
 * @date 2022/1/15
 */
public class HelpKeyWordCommandTest {

    HelpKeyWordCommand helpKeyWordCommand;

    @Before
    public void setUp() throws Exception {
        ResourceBundleMessageSource bundleMessageSource = new ResourceBundleMessageSource();
        bundleMessageSource.setBasename("i18n/messages");
        LocaleContextHolder.setLocale(Locale.ENGLISH);
        helpKeyWordCommand = new HelpKeyWordCommand(bundleMessageSource);
    }

    @Test
    public void execute() {
        HelpCommand statement = new HelpCommand(null, new Identifier("keyword"), Optional.of("comment"));
        InvokeResult<String> execute = helpKeyWordCommand.execute(statement);
        String data = execute.getData();
        assertEquals(data, "use `COMMENT` Define description");
    }

    @Test
    public void executeNotFound() {
        HelpCommand statement = new HelpCommand(null, new Identifier("keyword"), Optional.of("string"));
        InvokeResult<String> execute = helpKeyWordCommand.execute(statement);
        String data = execute.getData();
        assertEquals(data, "can not find `string` description");
    }

}