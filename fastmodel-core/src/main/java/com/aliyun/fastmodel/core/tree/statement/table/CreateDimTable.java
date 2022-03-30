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
 * 创建维度表:
 * <p>
 * DSL举例
 * <pre>
 * //普通维度
 * CREATE DIM TABLE IF NOT EXISTS hema.dim_product
 * (
 *   product_key string COMMENT '产品key',
 *   product_name string COMMENT '产品名称',
 *   product_address string COMMENT '产品产地',
 *   shop_key string comment '店铺',
 *   shop_name string comment '店铺名'
 *   primary key (product_key, shop_key)
 * )
 * COMMENT '商品纬度';
 * //层级维度
 * CREATE LEVEL DIM TABLE IF NOT EXISTS dim_level_area (
 *     area_key bigint primary key comment '地区key',
 *     country_key string comment '国家key',
 *     country_name string comment '国家名称',
 *     province_key string comment '省份key',
 *     province_name string comment '省份名称',
 *     city_key string comment '城市key',
 *     city_name string comment '城市名称',
 *     constraint level1 level &lt;country_key:(country_name),province_key, city_key&gt;
 * )  COMMENT '地区纬度';
 * //枚举维度
 * CREATE ENUM DIM TABLE IF NOT EXISTS dim_gender (
 *   code varchar(10) primary key comment 'key',
 *   value varchar(10) comment '值'
 * )
 * COMMENT '男女枚举纬度';
 * </pre>
 *
 * @author panguanjing
 * @date 2020/9/10
 */
public class CreateDimTable extends CreateTable {

    private CreateDimTable(DimTableBuilder builder) {
        super(builder);
    }

    public static DimTableBuilder builder() {
        return new DimTableBuilder();
    }

    public static class DimTableBuilder extends CreateTable.TableBuilder<DimTableBuilder> {
        public DimTableBuilder() {
            detailType = TableDetailType.NORMAL_DIM;
        }

        @Override
        public CreateDimTable build() {
            return new CreateDimTable(this);
        }
    }

}

