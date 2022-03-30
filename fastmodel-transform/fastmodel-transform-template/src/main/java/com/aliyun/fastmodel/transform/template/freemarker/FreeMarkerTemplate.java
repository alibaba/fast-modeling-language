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
import java.io.StringWriter;

import com.aliyun.fastmodel.transform.template.FmlTemplate;
import com.aliyun.fastmodel.transform.template.exception.FmlTemplateException;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * FreeMarker Template
 *
 * @author panguanjing
 * @date 2020/10/19
 */
public class FreeMarkerTemplate implements FmlTemplate {

    private final Template template;

    public FreeMarkerTemplate(Template template) {
        this.template = template;
    }

    @Override
    public String process(Object dataModel) {
        StringWriter out = new StringWriter();
        try {
            template.process(dataModel, out);
        } catch (TemplateException e) {
            throw new FmlTemplateException("template Process error", e);
        } catch (IOException e) {
            throw new FmlTemplateException("template IoException", e);
        }
        return out.toString();
    }
}
