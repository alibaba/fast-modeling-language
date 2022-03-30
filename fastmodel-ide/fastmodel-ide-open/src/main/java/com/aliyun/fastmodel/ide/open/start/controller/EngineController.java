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

package com.aliyun.fastmodel.ide.open.start.controller;

import java.util.Optional;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.ide.open.start.constants.ApiConstant;
import com.aliyun.fastmodel.ide.open.start.model.dto.FmlParamDTO;
import com.aliyun.fastmodel.ide.spi.command.CommandProvider;
import com.aliyun.fastmodel.ide.spi.command.IdeCommand;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * EngineController
 *
 * @author panguanjing
 * @date 2021/10/1
 */
@RestController
@RequestMapping(value = ApiConstant.ENGINE)
public class EngineController {

    private final CommandProvider commandProvider;

    public EngineController(CommandProvider commandProvider) {
        this.commandProvider = commandProvider;
    }

    @PostMapping(value = "get-image", produces = MediaType.IMAGE_PNG_VALUE)
    public @ResponseBody
    byte[] exportPng(FmlParamDTO plantUmlDTO) {
        ExportSql renderFml = new ExportSql(new Identifier(DialectName.PLANTUML.name()), Optional.empty(),
            Optional.of(plantUmlDTO.getFml()), null);
        IdeCommand ideCommand = commandProvider.getIdeCommand(renderFml);
        InvokeResult<byte[]> invokeResult = ideCommand.execute(renderFml);
        return invokeResult.getData();
    }
}
