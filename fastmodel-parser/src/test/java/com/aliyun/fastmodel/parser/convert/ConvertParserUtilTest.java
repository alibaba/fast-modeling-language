/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.convert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * 因为LSP不支持import方式, 将import中的文件和其他的文件合并到一期
 *
 * @author panguanjing
 * @date 2021/6/8
 */
public class ConvertParserUtilTest {
    String file = "src/main/antlr4/com/aliyun/fastmodel/parser/generate/FastModelGrammarParser.g4";

    String importDir = "src/main/antlr4/imports";

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
            if (old.startsWith(anImport)) {
                String substring = old.substring(anImport.length(), old.length() - 1);
                String[] split = substring.trim().split(",");
                importName = Lists.newArrayList(split);
            } else {
                afterLines.add(old);
            }
        }
        for (String i : importName) {
            String trim = i.trim();
            Path path1 = Paths.get(importDir, trim + ".g4");
            List<String> singleFile = IOUtils.readLines(
                new FileInputStream(new File(path1.toAbsolutePath().toString())),
                Charset.defaultCharset()
            );
            List<String> list = singleFile.stream().filter(x -> {
                return x.indexOf("parser grammar") == -1;
            }).collect(Collectors.toList());
            afterLines.addAll(list);
        }
        BaseConvertUtil.writeFile(afterLines, "FastModelGrammarParser.g4");
    }
}
