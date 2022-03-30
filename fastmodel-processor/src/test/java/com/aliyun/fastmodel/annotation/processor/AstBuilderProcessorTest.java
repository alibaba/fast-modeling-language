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

package com.aliyun.fastmodel.annotation.processor;

import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.TypeElement;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.mockito.BDDMockito.given;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/5/28
 */
public class AstBuilderProcessorTest {

    AstBuilderProcessor astBuilderProcessor;

    @Before
    public void setUp() throws Exception {
        astBuilderProcessor = new AstBuilderProcessor();
    }

    @Test
    public void processProcessOver() {
        RoundEnvironment roundEnvironment = Mockito.mock(RoundEnvironment.class);
        given(roundEnvironment.processingOver()).willReturn(true);
        boolean process = astBuilderProcessor.process(Sets.newHashSet(), roundEnvironment);
        assertFalse(process);
    }

    @Test
    public void processNotProcessOver() {
        RoundEnvironment roundEnvironment = Mockito.mock(RoundEnvironment.class);
        given(roundEnvironment.processingOver()).willReturn(false);
        TypeElement typeElement = Mockito.mock(TypeElement.class);
        boolean process = astBuilderProcessor.process(Sets.newHashSet(typeElement), roundEnvironment);
        assertFalse(process);
    }
}