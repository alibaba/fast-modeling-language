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

package com.aliyun.fastmodel.transform.hive.lsp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * 因为LSP不支持import方式, 将import中的文件和其他的文件合并到一期
 *
 * @author panguanjing
 * @date 2021/6/8
 */
public class ConvertParserUtilTest {
    String file = "src/main/antlr4/com/aliyun/fastmodel/transform/hive/parser/HiveParser.g4";

    String importDir = "src/main/antlr4/imports";

    public static final String output = "src/test/resources/lsp/";

    @Test
    public void testConvert() throws IOException {
        Path path = Paths.get("", file);
        List<String> lines = IOUtils.readLines(
            new FileInputStream(new File(path.toAbsolutePath().toString())),
            Charset.defaultCharset()
        );
        List<String> afterLines = new ArrayList<>();
        List<String> importName = null;
        for (String old : lines) {
            String anImport = "import";
            if (old.startsWith(anImport) && CollectionUtils.isEmpty(importName)) {
                String substring = old.substring(anImport.length(), old.length() - 1);
                String[] split = substring.trim().split(",");
                importName = Lists.newArrayList(split).stream().map(String::trim).collect(Collectors.toList());
            } else {
                afterLines.add(old);
            }
        }
        for (String i : importName) {
            Path path1 = Paths.get(importDir, i + ".g4");
            List<String> singleFile = IOUtils.readLines(
                new FileInputStream(new File(path1.toAbsolutePath().toString())),
                Charset.defaultCharset()
            );
            List<String> list = singleFile.stream().filter(x -> {
                return x.indexOf("parser grammar") == -1;
            }).collect(Collectors.toList());
            afterLines.addAll(list);
        }
        writeFile(afterLines, "HiveParser.g4");
    }

    public static void writeFile(List<String> newLines, String fileName) throws IOException {
        Path path1 = Paths.get("", output);
        IOUtils.writeLines(newLines, "\n", new FileOutputStream(
            new File(path1.toAbsolutePath().toString(), fileName)
        ), Charset.defaultCharset());
    }
}
