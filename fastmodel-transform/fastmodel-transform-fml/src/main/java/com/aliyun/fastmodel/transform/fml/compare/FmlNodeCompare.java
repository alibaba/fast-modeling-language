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

package com.aliyun.fastmodel.transform.fml.compare;

import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.compare.CompareResult;
import com.aliyun.fastmodel.transform.api.compare.NodeCompare;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.fml.FmlTransformer;
import com.google.auto.service.AutoService;

/**
 * NodeCompare
 *
 * @author panguanjing
 * @date 2021/9/4
 */
@Dialect(DialectName.FML)
@AutoService(NodeCompare.class)
public class FmlNodeCompare implements NodeCompare {

    private FmlTransformer fmlTransformer = new FmlTransformer();

    @Override
    public CompareResult compareResult(DialectNode before, DialectNode after, CompareContext context) {
        BaseStatement beforeStatement = (before != null && before.isExecutable()) ? fmlTransformer.reverse(before)
            : null;
        BaseStatement afterStatement = (after != null && after.isExecutable()) ? fmlTransformer.reverse(after) : null;
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(beforeStatement, afterStatement,
            context.getCompareStrategy());
        return new CompareResult(beforeStatement, afterStatement, compare);
    }

}
