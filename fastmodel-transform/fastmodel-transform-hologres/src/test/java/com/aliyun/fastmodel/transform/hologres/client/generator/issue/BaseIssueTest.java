/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator.issue;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.HologresTransformer;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/4/11
 */
public abstract class BaseIssueTest {
    protected HologresTransformer hologresTransformer = new HologresTransformer();

    public String generator(String text) {
        ReverseContext build = ReverseContext.builder().merge(true)
            .version(HologresVersion.V2)
            .build();
        BaseStatement reverse = hologresTransformer.reverse(new DialectNode(text), build);
        Table table = hologresTransformer.transformTable(reverse, TransformContext.builder().build());
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2)).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        DialectNode dialectNode = generate.getDialectNodes().get(0);
        return dialectNode.getNode();
    }

}
