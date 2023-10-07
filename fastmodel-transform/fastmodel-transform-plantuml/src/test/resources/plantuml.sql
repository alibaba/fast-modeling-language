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

CREATE
DIM TABLE dim_org
(
    org_id   bigint primary key comment '企业Id',
    org_name string comment '企业名称',
    list_data array <bigint>
)
COMMENT '企业维度表';

CREATE
FACT TABLE fact_t1 (
    org_id bigint comment '企业Id',
    constraint c1 dim references dim_org
) COMMENT  '人员变更事实表';


CREATE
INDICATOR idc_t1 bigint references fact_t1 comment '原子指标'
WITH PROPERTIES('type' = 'atomic') as sum(*);

