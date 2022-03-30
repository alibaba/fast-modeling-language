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

-- 创建业务板块：平台目前不支持，先手动insert，注意修改wq_bu为自己创建的业务板块
-- CREATE UNIT wq_bu comment '梧箐测试-业务板块';

-- 创建数据域
CREATE
DOMAIN dataworks.test_dm COMMENT "测试数据域";

-- 创建业务过程
CREATE
business_process dataworks.test_bp COMMENT "测试业务过程" WITH PROPERTIES('domain_key' = 'test_dm');

-- 创建维度表-门店
CREATE
DIM TABLE IF NOT EXISTS dataworks.dim_shop
(
  shop_code string COMMENT '门店code',
  shop_name string COMMENT '门店name',
  shop_type string COMMENT '门店类型',
  merchant_code bigint COMMENT '商家code',
  primary key (shop_code)
) COMMENT '门店' WITH PROPERTIES('type' = 'NORMAL', 'business_process'='test_bp');

-- 创建维度表-商品
CREATE
DIM TABLE IF NOT EXISTS dataworks.dim_sku
(
  sku_code string COMMENT '商品code',
  shop_code string COMMENT '门店code',
  sku_name string COMMENT '商品name',
  brand_code string COMMENT '品牌code',
  dept_code string COMMENT '部门code',
  cat_level_1_id string COMMENT '1级类目id',
  cat_level_2_id string COMMENT '2级类目id',
  cat_level_3_id string COMMENT '3级类目id',
  cat_level_4_id string COMMENT '4级类目id',
  primary key (sku_code,shop_code),
  constraint dim_sku_rel_dim_shop DIM KEY (shop_code) REFERENCES dim_shop(shop_code)
) COMMENT '商品' WITH PROPERTIES('type' = 'NORMAL', 'business_process'='test_bp');

-- 创建事实表-订单
CREATE
FACT TABLE IF NOT EXISTS dataworks.fact_pay_order
(
  order_id string COMMENT '订单id',
  sku_code string COMMENT '商品code',
  shop_code string COMMENT '门店code',
  gmt_create string COMMENT '创建时间',
  gmt_pay string COMMENT '支付时间',
  pay_type string COMMENT '支付类型',
  pay_price bigint COMMENT '支付金额',
  refund_price bigint COMMENT '退款金额',
  primary key (order_id),
  constraint fact_pay_order_rel_dim_sku DIM KEY (sku_code,shop_code) REFERENCES dim_sku(sku_code,shop_code),
  constraint fact_pay_order_rel_dim_shop DIM KEY (shop_code) REFERENCES dim_shop(shop_code)
) COMMENT '事实-支付订单' WITH PROPERTIES('type' = 'tx');

-- 创建原子指标-支付金额
CREATE
Indicator dataworks.pay_price bigint references fact_pay_order COMMENT '支付金额'
WITH PROPERTIES (
 'type' = 'ATOMIC'
,'date_field' = 'gmt_create'
,'date_field_format' = 'yyyy-mm-dd hh:mi:ss'
,'unit' = 'cny_yuan'
)
AS sum(fact_pay_order.pay_price);

-- 创建原子指标-下单次数
CREATE
Indicator dataworks.pay_count bigint references fact_pay_order COMMENT '下单次数'
WITH PROPERTIES (
 'type' = 'ATOMIC'
,'date_field' = 'gmt_create'
,'date_field_format' = 'yyyy-mm-dd hh:mi:ss'
,'unit' = 'ci'
)
AS sum(1);

-- 创建原子复合指标-平均支付金额
CREATE
INDICATOR dataworks.pay_price_avg DECIMAL
COMMENT '平均支付金额'
WITH PROPERTIES ('type'='ATOMIC_COMPOSITE', 'business_process'='test_bp')
AS pay_price/pay_count;

-- 创建带模板的原子指标
CREATE
Indicator dataworks.pay_price_v11 bigint references fact_pay_order COMMENT '支付金额'
WITH PROPERTIES (
 'type' = 'ATOMIC'
,'date_field' = 'gmt_create'
,'date_field_format' = 'yyyy-mm-dd hh:mi:ss'
,'unit' = 'cny_yuan'
,'template_columns' = 'xxx:fact_pay_order.order_id,yyy:fact_pay_order.sku_code'
)
AS sum(fact_pay_order.pay_price + #xxx# - #yyy# + #zzz#);

-- 填充为实现的模板
ALTER
INDICATOR dataworks.pay_price_v11 BIGINT
SET PROPERTIES(
      'template_columns' = 'xxx:fact_pay_order.order_id,yyy:fact_pay_order.sku_code,zzz:fact_pay_order.order_id'
)
AS sum(fact_pay_order.pay_price + #xxx# - #yyy# + #zzz#);

-- 创建派生指标-门店&商品_近1天_生鲜门店&生鲜类目_支付金额
CREATE
INDICATOR dataworks.shop_sku_1d_pay_price_001 BIGINT
references pay_price
comment '门店&商品_近1天_生鲜门店&生鲜类目_支付金额'
WITH PROPERTIES(
      'type' = 'DERIVATIVE',
      'dim'= 'dim_shop,dim_sku',
      'date_period'= 'd1',
      'filter_expr'= "dim_shop.shop_type = '1' and dim_sku.cat_level_1_id = '1234567' ",
      'filter_name'= '生鲜门店&生鲜类目'
);

-- 创建派生指标-门店&商品_近1天_生鲜门店&生鲜类目_支付次数
CREATE
INDICATOR dataworks.shop_sku_1d_pay_count_001 BIGINT
references pay_count
comment '门店&商品_近1天_生鲜门店&生鲜类目_支付次数'
WITH PROPERTIES(
      'type' = 'DERIVATIVE',
      'dim'= 'dim_shop,dim_sku',
      'date_period'= 'd1',
      'filter_expr'= "dim_shop.shop_type = '1' and dim_sku.cat_level_1_id = '1234567' ",
      'filter_name'= '生鲜门店&生鲜类目'
);

-- 创建派生复合指标-门店&商品_近1天_生鲜门店&生鲜类目_平均支付金额
CREATE
INDICATOR dataworks.shop_sku_1d_pay_price_avg DECIMAL
REFERENCES pay_price_avg
comment '门店&商品_近1天_生鲜门店&生鲜类目_平均支付金额'
WITH PROPERTIES(
      'type' = 'DERIVATIVE_COMPOSITE', 'business_process' = 'test_bp'
) AS (shop_sku_1d_pay_price_001 / shop_sku_1d_pay_count_001);

-- 物化表
CREATE
MATERIALIZED VIEW dataworks.phy_test_group
references (dim_shop, dim_sku, fact_pay_order)
comment '测试物化到ODPS'
engine odps
with properties('lifecycle'='365','partition_key'='ds');

-- 创建指标物化(930前先不支持复合指标的物化)
CREATE
MATERIALIZED VIEW dataworks.dws_shop_sku_d1_indicator_group_v100
references (shop_sku_1d_pay_price_001, shop_sku_1d_pay_count_001)
comment '门店商品近1天DWS指标宽表'
engine odps
with properties(
  'dim'='dim_sku,dim_shop'
, 'life_circle'='365'
, 'table_code'='dws_shop_sku_d1_indicator_v100'
, 'project_id'='autotest'
, 'file_path'='旧版工作流/demo/indicator'
);

-- ************************ 修改 ************************** --

-- 修改指标comment
ALTER
INDICATOR dataworks.pay_count set comment '下单次数-V2';

-- 修改原子指标pay_count
ALTER
INDICATOR dataworks.pay_count references fact_pay_order
SET PROPERTIES('bp_code' = 'test_bp')
AS count(fact_pay_order.order_id);

-- 修改原子复合指标pay_count
ALTER
INDICATOR dataworks.pay_price_avg
SET PROPERTIES('bp_code' = 'test_bp')
AS pay_count/pay_price;

-- 修改派生指标的过滤条件
ALTER
INDICATOR dataworks.shop_sku_1d_pay_count_001
SET PROPERTIES(
      'filter_expr'= "dim_shop.shop_type = '1'",
      'filter_name'= '生鲜门店&生鲜类目V2'
);

-- 新建一个原子指标V2
CREATE
Indicator dataworks.pay_count_v2 bigint references fact_pay_order COMMENT '下单次数V2'
WITH PROPERTIES (
 'type' = 'ATOMIC'
,'date_field' = 'gmt_create'
,'date_field_format' = 'yyyy-mm-dd hh:mi:ss'
)
AS sum(1);

-- 创建派生指标-门店&商品_近1天_生鲜门店&生鲜类目_支付次数V2
CREATE
INDICATOR dataworks.shop_sku_1d_pay_count_002 BIGINT
references pay_count
comment '门店&商品_近1天_生鲜门店&生鲜类目_支付次数V2'
WITH PROPERTIES(
      'type' = 'DERIVATIVE',
      'dim'= 'dim_shop,dim_sku',
      'date_period'= 'd1',
      'filter_expr'= "dim_shop.shop_type = '1' and dim_sku.cat_level_1_id = '1234567' ",
      'filter_name'= '生鲜门店&生鲜类目'
);

-- 修改派生指标的关联的原子指标
ALTER
INDICATOR dataworks.shop_sku_1d_pay_count_002 references pay_count_v2;

-- 修改全部信息
ALTER
INDICATOR dataworks.shop_sku_1d_pay_count_002 BIGINT
references pay_count
SET PROPERTIES(
      'type' = 'DERIVATIVE',
      'dim'= 'dim_sku',
      'date_period'= 'd7',
      'filter_expr'= "dim_sku.cat_level_1_id = '1234567' ",
      'filter_name'= '生鲜门店&生鲜类目'
);