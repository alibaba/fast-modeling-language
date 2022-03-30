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

package com.aliyun.fastmodel.transform.fml;

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.fml.context.FmlTransformContext;
import com.aliyun.fastmodel.transform.fml.format.FmlFormatter;
import com.google.auto.service.AutoService;

/**
 * 目前只支持FML原生语句内容，未来支持的
 *
 * @author panguanjing
 * @date 2021/1/28
 */
@Dialect(DialectName.FML)
@AutoService(Transformer.class)
public class FmlTransformer implements Transformer<BaseStatement> {
    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        FmlTransformContext fmlTransformContext = new FmlTransformContext(context);
        return FmlFormatter.formatNode(source, fmlTransformContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        List<BaseStatement> list = fastModelParser.multiParse(new DomainLanguage(dialectNode.getNode()));
        if (list.size() == 1) {
            return list.get(0);
        }
        return new CompositeStatement(list);
    }
}
