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

import java.io.ByteArrayOutputStream;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.ide.spi.command.BaseIdeCommand;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.plantuml.diagram.DiagramGenerator;
import org.springframework.stereotype.Component;

/**
 * EngineTransformCommand
 *
 * @author panguanjing
 * @date 2021/10/3
 */
@Component
public class RenderPlantUmlIdeCommand extends BaseIdeCommand<byte[], ExportSql> {

    @Override
    public InvokeResult<byte[]> execute(ExportSql renderFml) {
        Identifier dialect = renderFml.getDialect();
        DialectName byCode = DialectName.getByCode(dialect.getValue());
        DialectMeta dialectMeta = DialectMeta.getByName(byCode);
        String result = getIdePlatform().getToolService().exportSql(renderFml.getFml().get(), dialectMeta);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DiagramGenerator.exportPNG(result, outputStream);
        byte[] bytes = outputStream.toByteArray();
        return InvokeResult.success(bytes);
    }

    @Override
    public boolean isMatch(ExportSql baseStatement) {
        Identifier mode = baseStatement.getDialect();
        DialectName byCode = DialectName.getByCode(mode.getValue());
        return byCode == DialectName.PLANTUML;
    }
}
