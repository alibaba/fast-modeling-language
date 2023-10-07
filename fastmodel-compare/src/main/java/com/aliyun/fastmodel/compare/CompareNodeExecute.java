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

package com.aliyun.fastmodel.compare;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.fastmodel.compare.impl.BaseCompareNode;
import com.aliyun.fastmodel.compare.impl.CompositeCompareNode;
import com.aliyun.fastmodel.compare.impl.CreateTableCompareNode;
import com.aliyun.fastmodel.compare.impl.DropTableCompareNode;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateOdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * Node Execute
 *
 * @author panguanjing
 * @date 2021/2/5
 */
public class CompareNodeExecute {

    @Getter
    private Map<String, BaseCompareNode> maps = new ConcurrentHashMap<>(10);

    private CompareNodeExecute() {
        CreateTableCompareNode value = new CreateTableCompareNode();
        maps.put(CreateTable.class.getName(), value);
        maps.put(CreateOdsTable.class.getName(), value);
        maps.put(CreateDimTable.class.getName(), value);
        maps.put(CreateFactTable.class.getName(), value);
        maps.put(CreateCodeTable.class.getName(), value);
        maps.put(CreateDwsTable.class.getName(), value);
        maps.put(CreateAdsTable.class.getName(), value);
        DropTableCompareNode dropTableCompareNode = new DropTableCompareNode();
        maps.put(DropTable.class.getName(), dropTableCompareNode);
        CompositeCompareNode compositeCompareNode = new CompositeCompareNode(maps);
        maps.put(CompositeStatement.class.getName(), compositeCompareNode);
    }

    private static CompareNodeExecute INSTANCE = new CompareNodeExecute();

    public static CompareNodeExecute getInstance() {
        return INSTANCE;
    }

    /**
     * 比对
     *
     * @param before
     * @param after
     * @param strategy
     * @return
     */
    public List<BaseStatement> compareNode(Node before, Node after, CompareStrategy strategy) {
        boolean beforeNull = before == null;
        boolean afterNull = after == null;
        if (beforeNull && afterNull) {
            return ImmutableList.of();
        }
        Class<?> beforeClazz = null;
        Class<?> afterClazz = null;
        if (!beforeNull) {
            beforeClazz = before.getClass();
        }
        if (!afterNull) {
            afterClazz = after.getClass();
        }
        //只要其中有一个是复合statement，那么就按照compositeStatement的方式进行处理
        if (beforeClazz == CompositeStatement.class || afterClazz == CompositeStatement.class) {
            return maps.get(CompositeStatement.class.getName()).compareNode(before, after, strategy);
        }
        //取其中一个clazz对象内容
        Class clazz = beforeClazz == null ? afterClazz : beforeClazz;
        BaseCompareNode baseCompareNode = maps.get(clazz.getName());
        if (baseCompareNode == null) {
            throw new UnsupportedOperationException("Unsupported compare Node with class:" + clazz);
        }
        return baseCompareNode.compareNode(before, after, strategy);
    }

    /**
     * compare
     *
     * @param before
     * @param after
     * @param strategy
     * @return {@link BaseStatement}
     */
    public List<BaseStatement> compare(BaseStatement before, BaseStatement after, CompareStrategy strategy) {
        return compareNode(before, after, strategy);
    }
}

