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

package com.aliyun.fastmodel.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.aliyun.fastmodel.benchmarks.PerformanceExec;
import com.aliyun.fastmodel.benchmarks.PerformanceUtil;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.parser.NodeParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 性能测试信息内容
 *
 * @author panguanjing
 * @date 2021/3/31
 */
@Slf4j
@Ignore
public class PerformanceTest {
    NodeParser nodeParser = new NodeParser();

    String text;

    ExecutorService executorService;

    String singleText;

    @Before
    public void setUp() throws IOException {
        InputStream resourceAsStream = PerformanceTest.class.getResourceAsStream("/performance/dsl-test.txt");
        text = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);
        InputStream single = PerformanceTest.class.getResourceAsStream("/performance/single.txt");
        singleText = IOUtils.toString(single, StandardCharsets.UTF_8);
        executorService = PerformanceUtil.createExecutor(10, 0, 100000);
    }

    @Test
    public void testParse() {
        Long sum = 0L;
        int count = 1000;
        for (int i = 0; i < count; i++) {
            PerformanceExec exec = () -> nodeParser.multiParse(new DomainLanguage(text));
            Long cost = PerformanceUtil.getCost(exec, TimeUnit.MILLISECONDS);
            sum = sum + cost;
        }
        log.info("average :{}", sum / count);
    }

    @Test
    public void testMultiParse() {
        executeMultiText(text, 1000);
    }

    @Test
    public void testSingleParse() {
        executeMultiText(singleText, 1000);
    }

    private void executeMultiText(String input, int count) {
        final List<Long> costList = new ArrayList<>();
        List<Future> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PerformanceExec exec = () -> nodeParser.multiParse(new DomainLanguage(input));
            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    Long cost = PerformanceUtil.getCost(exec, TimeUnit.MILLISECONDS);
                    costList.add(cost);
                }
            };
            list.add(executorService.submit(runnable));
        }
        for (Future f : list) {
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        Long sum = 0L;
        for (Long l : costList) {
            if (l == null) {
                continue;
            }
            sum = sum + l;
        }
        log.info("average: {}", sum / count);
    }

    @Test
    public void testSingle() {
        Long sum = 0L;
        int count = 1000;
        for (int i = 0; i < count; i++) {
            PerformanceExec exec = () -> nodeParser.multiParse(new DomainLanguage(singleText));
            Long cost = PerformanceUtil.getCost(exec, TimeUnit.NANOSECONDS);
            sum = sum + cost;
        }
        log.info("average :{}", sum / count);
    }
}