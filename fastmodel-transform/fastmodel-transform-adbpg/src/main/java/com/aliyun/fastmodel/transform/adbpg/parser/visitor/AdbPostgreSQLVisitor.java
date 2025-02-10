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

package com.aliyun.fastmodel.transform.adbpg.parser.visitor;

import com.aliyun.fastmodel.transform.adbpg.parser.tree.AdbPostgreSQLPartitionBy;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.AdbPostgreSQLRangeElement;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionIntervalExpression;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionValueExpression;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;

/**
 * AdbPgVisitor
 *
 * @author panguanjing
 * @date 2024/10/14
 */
public interface AdbPostgreSQLVisitor<R, C> extends PostgreSQLVisitor<R, C> {

    /**
     * visit adbPostgreSQLPartitionBy
     *
     * @param adbPostgreSQLPartitionBy
     * @param context
     * @return
     */
    default R visitAdbPostgreSQLPartitionBy(AdbPostgreSQLPartitionBy adbPostgreSQLPartitionBy, C context) {
        return visitNode(adbPostgreSQLPartitionBy, context);
    }

    /**
     * visitPartitionValueExpression
     *
     * @param partitionValueExpression
     * @param context
     * @return
     */
    default R visitPartitionValueExpression(PartitionValueExpression partitionValueExpression, C context) {
        return visitNode(partitionValueExpression, context);
    }

    /**
     * visitPartitionIntervalExpression
     *
     * @param partitionIntervalExpression
     * @param context
     * @return
     */
    default R visitPartitionIntervalExpression(PartitionIntervalExpression partitionIntervalExpression, C context) {
        return visitNode(partitionIntervalExpression, context);
    }

    /**
     * visitAdbPostgreSQLRangeElement
     *
     * @param adbPostgreSQLRangeElement
     * @param context
     * @return
     */
    default R visitAdbPostgreSQLRangeElement(AdbPostgreSQLRangeElement adbPostgreSQLRangeElement, C context) {
        return visitNode(adbPostgreSQLRangeElement, context);
    }
}
