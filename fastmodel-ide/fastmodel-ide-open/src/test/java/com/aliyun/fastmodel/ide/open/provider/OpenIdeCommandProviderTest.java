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

package com.aliyun.fastmodel.ide.open.provider;

import java.util.Map;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.core.tree.statement.command.ImportSql;
import com.aliyun.fastmodel.ide.open.extension.OpenIdeCommandProvider;
import com.aliyun.fastmodel.ide.open.extension.command.RenderPlantUmlIdeCommand;
import com.aliyun.fastmodel.ide.spi.command.ExportSqlIdeCommand;
import com.aliyun.fastmodel.ide.spi.command.IdeCommand;
import com.aliyun.fastmodel.ide.spi.command.ImportSqlIdeCommand;
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

/**
 * OpenIdeCommandProviderTest
 *
 * @author panguanjing
 * @date 2021/12/5
 */
@RunWith(MockitoJUnitRunner.class)
public class OpenIdeCommandProviderTest {
    OpenIdeCommandProvider openIdeCommandProvider;

    @Mock
    private IdePlatform idePlatform;

    @Mock
    ApplicationContext applicationContext;

    @Before
    public void setUp() throws Exception {
        openIdeCommandProvider = new OpenIdeCommandProvider(idePlatform);
        openIdeCommandProvider.setApplicationContext(applicationContext);
        Map<String, IdeCommand> maps = Maps.newHashMap();
        maps.put(ExportSql.class.getName(), new RenderPlantUmlIdeCommand());
        given(applicationContext.getBeansOfType(IdeCommand.class)).willReturn(maps);
        openIdeCommandProvider.init();
    }

    @Test
    public void testGetCommand() {
        ImportSql importSql = new ImportSql(new Identifier("mysql"), null,
            Optional.of("create table a (b bigint comment 'comment')"), null);
        IdeCommand ideCommand = openIdeCommandProvider.getIdeCommand(importSql);
        assertEquals(ideCommand.getClass().getName(), ImportSqlIdeCommand.class.getName());
    }

    @Test
    public void testGetCommandExportSql() {
        ExportSql importSql = new ExportSql(new Identifier("mysql"), null,
            Optional.of("create table a (b bigint comment 'comment')"), null);
        IdeCommand ideCommand = openIdeCommandProvider.getIdeCommand(importSql);
        assertEquals(ideCommand.getClass().getName(), ExportSqlIdeCommand.class.getName());
    }

    @Test
    public void testGetCommandRenderFmlPlantUml() {
        ExportSql importSql = new ExportSql(new Identifier("plantuml"), null,
            Optional.of("create table a (b bigint comment 'comment')"), null);
        IdeCommand ideCommand = openIdeCommandProvider.getIdeCommand(importSql);
        assertEquals(ideCommand.getClass().getName(), RenderPlantUmlIdeCommand.class.getName());
    }
}