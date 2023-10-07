/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.convert;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.parser.StatementSplitter;
import com.aliyun.fastmodel.parser.StatementSplitter.Item;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * 用于生成lsp的关键词的工具，目前都是部分的关键字的内容不太方便
 * https://configuration.lsp.alibaba-inc.com/index#/edit/84
 *
 * @author panguanjing
 * @date 2021/3/30
 */
public class KeyWordsGeneratorTest {

    String format = "%s,%s,%s,14";

    @Test
    public void generate() throws IOException {
        Set<Item> keywords = StatementSplitter.getVocabularyItems();
        List<String> list = keywords.stream().map(
            x -> String.format(format, x.getSymbolName(), x.getSymbolName(), x.getKeyword())
        ).collect(Collectors.toList());
        BaseConvertUtil.writeFile(list, "keyword.txt");
    }

    @Test
    public void testJson() throws IOException {

        Set<Item> keywords = StatementSplitter.getVocabularyItems();
        List<String> keyword = keywords.stream().map(
            x -> {
                return x.getKeyword();
            }
        ).collect(Collectors.toList());
        Path path = Paths.get(BaseConvertUtil.OUTPUT, "highlight.json");
        List<String> lines = IOUtils.readLines(
            new FileInputStream(path.toFile()),
            Charset.defaultCharset()
        );
        List<String> list = new ArrayList<>();
        for (String line : lines) {
            String trim = line.trim();
            String prefix = "keywords";
            if (trim.startsWith(prefix)) {
                String format = "\tkeywords:[%s],";
                String collect = keyword.stream().map(x -> {
                    return "\"" + x + "\"";
                }).collect(Collectors.joining(","));
                list.add(String.format(format, collect));
            } else {
                list.add(line);
            }
        }
        BaseConvertUtil.writeFile(list, "highlight.json");
    }
}

