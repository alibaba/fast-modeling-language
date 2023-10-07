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

package com.aliyun.fastmodel.transform.example.integrate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.io.IOUtils;

/**
 * transformer integrate test
 *
 * @author panguanjing
 * @date 2021/7/8
 */
public abstract class AbstractTransformIntegrate {

    FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();

    /**
     * get expect content
     *
     * @return
     */
    public abstract String getExpect();

    /**
     * get dialect transformer {@link Transformer}
     *
     * @return
     */
    public abstract Transformer getTransformer();

    public String execute(TransformContext transformContext) throws IOException {
        String fml = IOUtils.resourceToString("/example/source.fml", StandardCharsets.UTF_8);
        List<BaseStatement> baseStatements = fastModelParser.multiParse(new DomainLanguage(fml));
        List<String> list = new ArrayList<>();
        for (BaseStatement baseStatement : baseStatements) {
            DialectNode dialectNode = getTransformer().transform(baseStatement, transformContext);
            list.add(dialectNode.getNode());
        }
        String collect = list.stream().collect(Collectors.joining(";\n"));
        return collect;
    }
}
