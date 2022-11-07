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

package com.aliyun.fastmodel.transform.mysql.compare;

import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.compare.CompareResult;
import com.aliyun.fastmodel.transform.api.compare.NodeCompare;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mysql.MysqlTransformer;
import com.google.auto.service.AutoService;

/**
 * mysql的节点比对操作内容
 *
 * @author panguanjing
 * @date 2021/8/29
 */
@Dialect(DialectName.Constants.MYSQL)
@AutoService(NodeCompare.class)
public class MysqlNodeCompare implements NodeCompare {

    private MysqlTransformer mysqlV8Transformer = new MysqlTransformer();

    @Override
    public CompareResult compareResult(DialectNode before, DialectNode after, CompareContext context) {
        BaseStatement beforeStatement = (before != null && before.isExecutable()) ? mysqlV8Transformer.reverse(before) : null;
        BaseStatement afterStatement = (after != null && after.isExecutable()) ? mysqlV8Transformer.reverse(after) : null;
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(beforeStatement, afterStatement,
            context.getCompareStrategy());
        return new CompareResult(beforeStatement, afterStatement, compare);
    }
}
