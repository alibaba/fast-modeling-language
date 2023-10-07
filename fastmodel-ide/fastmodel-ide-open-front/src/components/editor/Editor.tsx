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

import * as React from 'react';
import * as monaco from 'monaco-editor-core';
import {editor} from 'monaco-editor-core';
import IModelContentChangedEvent = editor.IModelContentChangedEvent;

interface IEditorPorps {
    language: string;
    didChangeContent: (e: IModelContentChangedEvent) => void
    initContent: () => void
}

const newValue = " CREATE DIM TABLE IF NOT EXISTS dim_shop\n" +
    "(\n" +
    "  shop_code string COMMENT '门店code',\n" +
    "  shop_name string COMMENT '门店name',\n" +
    "  shop_type string COMMENT '门店类型',\n" +
    "  merchant_code bigint COMMENT '商家code',\n" +
    "  primary key (shop_code)\n" +
    ") COMMENT '门店';\n" +
    "\n" +
    "CREATE DIM TABLE IF NOT EXISTS dim_sku\n" +
    "(\n" +
    "  sku_code string COMMENT '商品code',\n" +
    "  shop_code string COMMENT '门店code',\n" +
    "  sku_name string COMMENT '商品name',\n" +
    "  brand_code string COMMENT '品牌code',\n" +
    "  dept_code string COMMENT '部门code',\n" +
    "  cat_level_1_id string COMMENT '1级类目id',\n" +
    "  cat_level_2_id string COMMENT '2级类目id',\n" +
    "  cat_level_3_id string COMMENT '3级类目id',\n" +
    "  cat_level_4_id string COMMENT '4级类目id',\n" +
    "  primary key (sku_code,shop_code)\n" +
    ") COMMENT '商品';\n" +
    "\n" +
    "\n" +
    "CREATE FACT TABLE IF NOT EXISTS fact_pay_order\n" +
    "(\n" +
    "  order_id string COMMENT '订单id',\n" +
    "  sku_code string COMMENT '商品code',\n" +
    "  shop_code string COMMENT '门店code',\n" +
    "  gmt_create string COMMENT '创建时间',\n" +
    "  gmt_pay string COMMENT '支付时间',\n" +
    "  pay_type string COMMENT '支付类型',\n" +
    "  pay_price bigint COMMENT '支付金额',\n" +
    "  refund_price bigint COMMENT '退款金额',\n" +
    "  primary key (order_id)\n" +
    ") COMMENT '事实-支付订单';\n" +
    "\n" +
    "\n" +
    "REF fact_pay_order.sku_code -> dim_sku.sku_code : skuCode;\n" +
    "REF fact_pay_order.shop_code -> dim_shop.shop_code :shopCode;";

const Editor: React.FC<IEditorPorps> = (props: IEditorPorps) => {
    let divNode;

    const assignRef = React.useCallback((node) => {
        // On mount get the ref of the div and assign it the divNode
        divNode = node;
    }, []);

    React.useEffect(() => {
        if (divNode) {
            const editor = monaco.editor.create(divNode, {
                language: props.language,
                minimap: {enabled: false},
                autoIndent: true
            });
            editor.setValue(newValue);
            editor.getModel().onDidChangeContent(props.didChangeContent);
            props.initContent();
        }
    }, [assignRef])

    return <div ref={assignRef} style={{height: '90vh'}} onChange={props.initContent}></div>;
}

export {Editor};