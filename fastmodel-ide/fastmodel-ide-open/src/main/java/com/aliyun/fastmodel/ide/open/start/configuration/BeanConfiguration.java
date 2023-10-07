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

package com.aliyun.fastmodel.ide.open.start.configuration;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.ide.spi.command.CommandProvider;
import com.aliyun.fastmodel.ide.spi.invoker.DefaultIdeInvoker;
import com.aliyun.fastmodel.ide.spi.invoker.IdeInvoker;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.compare.NodeCompareFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ResourceBundleMessageSource;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/10/1
 */
@Configuration
public class BeanConfiguration {

    @Bean
    public FastModelParser fastModelParser() {
        return FastModelParserFactory.getInstance().get();
    }

    @Bean
    public NodeCompareFactory nodeCompareFactory() {
        return NodeCompareFactory.getInstance();
    }

    @Bean
    public TransformerFactory transformerFactory() {
        return TransformerFactory.getInstance();
    }

    @Bean
    public IdeInvoker ideInvoker(@Autowired CommandProvider commandProvider) {
        return new DefaultIdeInvoker(commandProvider);
    }

    @Bean(name = "messageSource")
    public ResourceBundleMessageSource getMessageSource() {
        ResourceBundleMessageSource resourceBundleMessageSource = new ResourceBundleMessageSource();
        resourceBundleMessageSource.setDefaultEncoding("UTF-8");
        resourceBundleMessageSource.setBasenames("i18n/messages");
        resourceBundleMessageSource.setFallbackToSystemLocale(false);
        return resourceBundleMessageSource;
    }

}
