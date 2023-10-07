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

package com.aliyun.fastmodel.ide.spi.command;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.google.auto.service.AutoService;

/**
 * export sql
 *
 * @author panguanjing
 * @date 2021/12/3
 */
@AutoService(IdeCommand.class)
public class ExportSqlIdeCommand extends BaseIdeCommand<String, ExportSql> {

    @Override
    public InvokeResult<String> execute(ExportSql baseStatement) {
        Identifier dialect = baseStatement.getDialect();
        DialectMeta dialectMeta = DialectMeta.getByName(DialectName.getByCode(dialect.getValue()));
        String result = this.getIdePlatform().getToolService().exportSql(baseStatement.getFml().get(), dialectMeta);
        return InvokeResult.success(result);
    }

}
