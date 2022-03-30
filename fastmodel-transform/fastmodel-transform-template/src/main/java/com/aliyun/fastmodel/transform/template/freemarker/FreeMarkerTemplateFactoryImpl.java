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

package com.aliyun.fastmodel.transform.template.freemarker;

import java.io.IOException;

import com.aliyun.fastmodel.transform.template.FmlTemplate;
import com.aliyun.fastmodel.transform.template.FmlTemplateFactory;
import com.aliyun.fastmodel.transform.template.exception.FmlTemplateException;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/19
 */
public class FreeMarkerTemplateFactoryImpl implements FmlTemplateFactory {
    private static final Configuration CONFIGURATION = new Configuration(
        Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);

    private static Template get(String name) throws IOException {
        CONFIGURATION.setLogTemplateExceptions(true);
        ClassTemplateLoader classTemplateLoader = new ClassTemplateLoader(FmlTemplateFactory.class, "/");
        CONFIGURATION.setTemplateLoader(classTemplateLoader);
        return CONFIGURATION.getTemplate(name);
    }

    @Override
    public FmlTemplate getTemplate(String relativePath) throws FmlTemplateException {
        try {
            Template f = get(relativePath);
            return new FreeMarkerTemplate(f);
        } catch (Exception e) {
            throw new FmlTemplateException("getTemplate Occur Exception:" + relativePath, e);
        }
    }
}
