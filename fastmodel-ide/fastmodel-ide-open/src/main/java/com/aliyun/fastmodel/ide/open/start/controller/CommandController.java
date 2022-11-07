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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.core.tree.statement.command.ImportSql;
import com.aliyun.fastmodel.ide.open.service.StorageService;
import com.aliyun.fastmodel.ide.open.start.constants.ApiConstant;
import com.aliyun.fastmodel.ide.open.start.model.dto.ExportParamDTO;
import com.aliyun.fastmodel.ide.open.start.model.dto.FmlParamDTO;
import com.aliyun.fastmodel.ide.open.start.model.dto.ImportParamDTO;
import com.aliyun.fastmodel.ide.spi.command.CommandProvider;
import com.aliyun.fastmodel.ide.spi.command.IdeCommand;
import com.aliyun.fastmodel.ide.spi.exception.PlatformException;
import com.aliyun.fastmodel.ide.spi.exception.error.PlatformErrorCode;
import com.aliyun.fastmodel.ide.spi.invoker.IdeInvoker;
import com.aliyun.fastmodel.ide.spi.invoker.InvokeResult;
import com.aliyun.fastmodel.ide.spi.params.FmlParams;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.google.common.collect.ImmutableList;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * CommandController
 *
 * @author panguanjing
 * @date 2021/12/21
 */
@RestController
@RequestMapping(value = ApiConstant.ENGINE)
public class CommandController {

    private final CommandProvider commandProvider;

    private final IdeInvoker ideInvoker;

    private final StorageService storage;

    public CommandController(CommandProvider commandProvider, IdeInvoker ideInvoker, StorageService storage) {
        this.commandProvider = commandProvider;
        this.ideInvoker = ideInvoker;
        this.storage = storage;
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

    @PostMapping("/execute")
    public InvokeResult execute(FmlParams fmlParams) {
        return ideInvoker.invoke(fmlParams);
    }

    @PostMapping("/import_sql")
    public InvokeResult<String> importSqlCommand(ImportParamDTO importParam) {
        ImportSql importSql = new ImportSql(new Identifier(importParam.getDialectName()), Optional.empty(),
            Optional.of(importParam.getText()), ImmutableList.of());
        return commandProvider.getIdeCommand(importSql).execute(importSql);
    }

    @PostMapping("/export_sql")
    public InvokeResult<String> exportSqlCommand(ExportParamDTO exportParamDTO) {
        ExportSql importSql = new ExportSql(new Identifier(exportParamDTO.getDialectName()), Optional.empty(),
            Optional.of(exportParamDTO.getFml()), ImmutableList.of());
        return commandProvider.getIdeCommand(importSql).execute(importSql);
    }

    @PostMapping("plantuml")
    @ResponseBody
    public InvokeResult plantUml(FmlParamDTO plantUmlDTO) {
        ExportSql exportSql = new ExportSql(new Identifier(DialectName.PLANTUML.name()), Optional.empty(),
            Optional.of(plantUmlDTO.getFml()), ImmutableList.of());
        IdeCommand ideCommand = commandProvider.getIdeCommand(exportSql);
        return ideCommand.execute(exportSql);
    }

    @PostMapping("x6")
    @ResponseBody
    public InvokeResult<String> x6(FmlParamDTO paramDTO) {
        ExportSql exportSql = new ExportSql(new Identifier(DialectName.GRAPH.name()), Optional.empty(),
            Optional.of(paramDTO.getFml()), ImmutableList.of());
        IdeCommand ideCommand = commandProvider.getIdeCommand(exportSql);
        return ideCommand.execute(exportSql);
    }

    @PostMapping("/upload_sql")
    public InvokeResult<String> importSqlByFile(String dialectName, MultipartFile file) {
        ImportSql importSql = null;
        Resource save = storage.save(file);
        try (Reader reader = new InputStreamReader(save.getInputStream(), UTF_8)) {
            String text = FileCopyUtils.copyToString(reader);
            return InvokeResult.success(text);
        } catch (IOException e) {
            throw new PlatformException("read string error:" + file.getOriginalFilename(),
                PlatformErrorCode.READ_FILE_ERROR, e);
        }
    }
}
