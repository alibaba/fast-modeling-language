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

package com.aliyun.fastmodel.core.tree.statement.table;

import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;

/**
 * 创建事实表
 * <p>
 * DSL举例：
 * <pre>
 *
 *  CREATE FACT TABLE IF NOT EXISTS fact_trade_pay(
 *    order_id bigint comment '订单ID',
 *    pay_date_key bigint,
 *    product_key  string,
 *    price decimal comment '下单金额',
 *    shop_key string comment '店铺key',
 *    city_key string comment '城市key',
 *    constraint dim1 DIM KEY pay_date_key REFERENCES dim_pay_date(pay_date_key) comment '日期key'
 *    constraint dim2 DIM KEY shop_key REFERENCES dim_shop(shop_key) comment '门店纬度',
 *    constraint dim3 DIM REFERENCES dim_area comment '关联地区纬度'
 * )
 * COMMENT '交易下单事实表'
 * </pre>
 *
 * @author panguanjing
 * @date 2020/9/10
 */
public class CreateFactTable extends CreateTable {

    private CreateFactTable(FactTableBuilder factTableBuilder) {
        super(factTableBuilder);
    }

    public static FactTableBuilder builder() {
        return new FactTableBuilder();
    }

    public static class FactTableBuilder extends TableBuilder<FactTableBuilder> {

        public FactTableBuilder() {
            detailType = TableDetailType.TRANSACTION_FACT;
        }

        @Override
        public CreateFactTable build() {
            return new CreateFactTable(this);
        }
    }
}

