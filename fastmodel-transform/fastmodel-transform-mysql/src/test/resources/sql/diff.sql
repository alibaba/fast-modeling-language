# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`adjunct`
    ADD INDEX idx_adjunct_name (name),
    COMMENT ='修饰词配置表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`business_process`
    ADD COLUMN parent_code varchar(128) NULL COMMENT '父节点code' AFTER source,
    COMMENT ='业务过程';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`business_unit`
    COMMENT ='业务板块';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`check_result`
    COMMENT ='检查结果表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`checker`
    ADD COLUMN is_default       tinyint(4)    NULL COMMENT '是否是主校验规则' AFTER is_real,
    ADD COLUMN expression_value varchar(2048) NULL COMMENT '表达式的自定义的值' AFTER is_default,
    COMMENT ='数仓分层检查器表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`code_table`
    COMMENT ='码表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`code_table_column`
    COMMENT ='码表列';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`code_table_content`
    COMMENT ='码表枚举内容';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`data_dictionary`
    COMMENT ='数据字典表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`data_domain`
    COMMENT ='数据域';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`data_warehouse_layer`
    COMMENT ='数仓分层表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`diagram`
    COMMENT ='画布表';



# WARNING: Using a password on the command line interface can be insecure.


# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`dimension_level`
    COMMENT ='维度层级';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`directory`
    ADD COLUMN scope text NULL COMMENT '约束范围，比如：业务分类下的数据域' AFTER order_id,
    COMMENT ='目录表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`distributed_lock`
    COMMENT ='分布式锁';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`dws_query_config`
    COMMENT ='汇总表取数配置';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`entity_relation`
    COMMENT ='领域模型关联关系表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`enum_table_content`
    COMMENT ='枚举维度表内容';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`indicator_atomic`
    ADD INDEX idx_indicator_atomic_name (name),
    COMMENT ='原子指标配置表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`indicator_derivative`
    CHANGE COLUMN dws_table_uuid dws_table_uuid               varchar(64) NULL COMMENT '废弃——所属汇总逻辑表uuid' AFTER build_type,
    CHANGE COLUMN ignore_dimension_tag ignore_dimension_tag   tinyint(4)  NULL DEFAULT '0' COMMENT '废弃——是否忽略维度' AFTER dws_table_column_uuid,
    CHANGE COLUMN dws_table_column_uuid dws_table_column_uuid varchar(64) NULL COMMENT '废弃——所属汇总逻辑表字段uuid' AFTER dws_table_uuid,
    COMMENT ='衍生指标配置表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`indicator_dimension`
    COMMENT ='指标维度';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`indicator_rel_dws`
    COMMENT ='指标关联汇总逻辑表信息';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`log_event`
    COMMENT ='日志表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`log_event_exception`
    COMMENT ='日志异常类';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`measure_unit`
    COMMENT ='度量单位';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`model_column`
    ADD UNIQUE INDEX uk_uuid (uuid),
    COMMENT ='维度建模表字段';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`model_column_group`
    COMMENT ='维度建模表字段组';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`model_table`
    ADD COLUMN storage_policy_code varchar(64) NULL COMMENT '存储策略编码' AFTER life_cycle,
    COMMENT ='维度建模表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`online_dqc_rule`
    ADD COLUMN model_code varchar(128) NULL COMMENT '模型code' AFTER property_type,
    COMMENT ='在线dqc配置表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`physical_entity_instance`
    COMMENT ='物化示例对象信息';



# WARNING: Using a password on the command line interface can be insecure.


# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`relationship`
    COMMENT ='关联关系';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`t_online_model`
    ADD COLUMN model_code                                     varchar(128) NULL COMMENT '模型code' AFTER source_project_id,
    ADD COLUMN source_project_id                              varchar(128) NULL COMMENT '源项目id' AFTER modifier_name,
    CHANGE COLUMN business_process_name business_process_name varchar(70)  NULL COMMENT '业务过程名称',
    COMMENT ='在线模型表';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`t_release_package`
    COMMENT ='发布包';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`t_release_package_object`
    CHANGE COLUMN need_realease_fml need_realease_fml mediumtext   NOT NULL COMMENT '需要发布的fml语句',
    CHANGE COLUMN big_engine_dsl big_engine_dsl       mediumtext   NULL COMMENT '引擎方言的dsl',
    ADD COLUMN release_object_type                    varchar(64)  NULL COMMENT '部署对象类型' AFTER dqc_rule,
    ADD COLUMN source_project_id                      varchar(128) NULL COMMENT '源项目id' AFTER release_object_type,
    COMMENT ='发布包要执行的发布对象';



# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`t_vc_commit_object`
    ADD COLUMN model_version                                  int(11)      NOT NULL DEFAULT '0' COMMENT '模型版本' AFTER release_object_id,
    ADD COLUMN model_code                                     varchar(128) NULL COMMENT '模型code' AFTER model_version,
    CHANGE COLUMN business_process_name business_process_name varchar(70)  NULL COMMENT '业务过程名称',
    COMMENT ='修饰词配置表';



# WARNING: Using a password on the command line interface can be insecure.


# WARNING: Using a password on the command line interface can be insecure.
# Transformation for --changes-for=server1:
#

ALTER TABLE `modelengineold`.`time_period`
    ADD INDEX idx_time_period_name (name),
    COMMENT ='时间周期配置表';



CREATE TABLE `config`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n `namespace` varchar(128) NOT NULL COMMENT '空间',\n `m_key` varchar(256) NOT NULL COMMENT '配置分组，key是数据库保留字，不采用',\n `value` text NOT NULL COMMENT '配置内容',\n `creator_id` varchar(64) NOT NULL COMMENT '创建者id',\n `creator_name` varchar(64) NOT NULL COMMENT '创建者名称',\n `modifier_id` varchar(64) DEFAULT NULL COMMENT '修改者id',\n `modifier_name` varchar(64) DEFAULT NULL COMMENT '修改者名称',\n `is_deleted` tinyint(4) NOT NULL COMMENT '是否删除',\n `tenant_id` varchar(64) NOT NULL COMMENT '当前租户id',\n `business_unit_uuid` varchar(64) NOT NULL COMMENT '业务板块uuid',\n PRIMARY KEY (`id`),\n KEY `idx_namespace_key` (`tenant_id`,`business_unit_uuid`,`namespace`,`m_key`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='配置中心';


CREATE TABLE `design_project`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n `design_project_id` varchar(64) NOT NULL COMMENT '设计空间id',\n `project_id` varchar(64) NOT NULL COMMENT '物化空间id',\n `modifier_id` varchar(64) NOT NULL COMMENT '修改者id',\n `modifier_name` varchar(64) NOT NULL COMMENT '修改者名称',\n `creator_id` varchar(64) NOT NULL COMMENT '创建者id',\n `creator_name` varchar(64) NOT NULL COMMENT '创建者名称',\n `is_deleted` tinyint(4) NOT NULL COMMENT '是否删除',\n `project_identifier` varchar(128) NOT NULL COMMENT '项目空间标识',\n `design_project_identifier` varchar(128) NOT NULL COMMENT '设计空间标识',\n `tenant_id` varchar(64) NOT NULL COMMENT '租户id',\n PRIMARY KEY (`id`),\n KEY `idx_design_materialize` (`design_project_id`,`project_id`),\n KEY `idx_materialize_design` (`project_id`,`design_project_id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='设计空间关系';


CREATE TABLE `design_project_record`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n `project_name` varchar(1024) NOT NULL COMMENT '工作空间中文名',\n `project_identifier` varchar(128) NOT NULL COMMENT '工作空间英文名',\n `modifier_id` varchar(128) NOT NULL COMMENT '变更操作人id',\n `modifier_name` varchar(128) NOT NULL COMMENT '变更人名称',\n `project_id` varchar(128) NOT NULL COMMENT '项目空间id',\n `type` tinyint(4) NOT NULL COMMENT '操作方式',\n `design_project_id` varchar(128) NOT NULL COMMENT '设计空间id',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='设计空间变更记录';


CREATE TABLE `distributed_config`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `namespace` varchar(128) DEFAULT NULL COMMENT '配置空间',\n `config_key` varchar(64) DEFAULT NULL COMMENT '配置key',\n `config_val` json DEFAULT NULL COMMENT '配置value',\n `modifier_id` varchar(64) DEFAULT NULL COMMENT '最近修改人id',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_distributed_config` (`config_key`,`namespace`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='分布式配置';


CREATE TABLE `distributed_task`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n `gmt_expire` datetime DEFAULT NULL COMMENT '过期时间',\n `namespace` varchar(128) NOT NULL COMMENT 'namespace',\n `task_id` varchar(64) NOT NULL COMMENT '任务id',\n `task_type` varchar(64) NOT NULL COMMENT '任务类型',\n `task_status` varchar(16) NOT NULL COMMENT '任务状态',\n `owner_id` varchar(64) DEFAULT NULL COMMENT '负责人id',\n `owner_name` varchar(64) DEFAULT NULL COMMENT '负责人名称',\n `params` longtext COMMENT '任务参数',\n `result` longtext COMMENT '任务结果',\n `error_msg` text COMMENT '错误信息',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_distributed_task` (`task_id`,`namespace`),\n KEY `idx_distributed_task_gmt_expire` (`gmt_expire`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='分布式任务管理';


CREATE TABLE `naming_dictionary`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `uuid` varchar(64) NOT NULL COMMENT '全局唯一的uuid',\n `code` varchar(128) DEFAULT NULL COMMENT 'code',\n `name` varchar(2048) DEFAULT NULL COMMENT 'name',\n `extend_name` varchar(2048) DEFAULT NULL COMMENT '扩展名称',\n `type` tinyint(4) NOT NULL COMMENT '类型',\n `business_unit_uuid` varchar(64) DEFAULT NULL COMMENT '所属业务板块uuid',\n `tenant_id` varchar(64) DEFAULT NULL COMMENT '租户id',\n `creator_id` varchar(128) DEFAULT NULL COMMENT '创建人租户baseId',\n `creator_name` varchar(128) DEFAULT NULL COMMENT '创建人显示名称',\n `modifier_id` varchar(128) DEFAULT NULL COMMENT '修改人租户baseId',\n `modifier_name` varchar(128) DEFAULT NULL COMMENT '修改人显示名称',\n `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_uuid` (`uuid`),\n KEY `idx_unit_code` (`business_unit_uuid`,`code`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 4
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='命名词典';


CREATE TABLE `onedata_adjunct_word`
(
    \n `adjunct_word_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '修饰词的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属业务板块的id',\n `data_domain_id` int(11) NOT NULL COMMENT '所属数据域的id',\n `adjunct_type_id` int(11) NOT NULL COMMENT '修饰类型的id',\n `adjunct_word_name` varchar(32) NOT NULL COMMENT '修饰词的英文名',\n `adjunct_word_name_cn` varchar(128) NOT NULL COMMENT '修饰词的中文名',\n `adjunct_word_comment` text COMMENT '修饰词的描述信息',\n `adjunct_word_creator` varchar(256) DEFAULT NULL COMMENT '修饰词的创建者',\n `adjunct_word_updater` varchar(256) DEFAULT NULL COMMENT '修饰词的最近更新者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '最新更新时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过，修饰词只有草稿和审核通过状态',\n PRIMARY KEY (`adjunct_word_id`),\n KEY `idx_def_adjunct_word_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_adjunct_word_data_domain_id` (`data_domain_id`),\n KEY `idx_def_adjunct_word_adjunct_type_id` (`adjunct_type_id`),\n KEY `idx_def_adjunct_word_adjunct_name_cn` (`adjunct_word_name_cn`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 4014087
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata数据同步－修饰词';


CREATE TABLE `onedata_atom_index`
(
    \n `atom_index_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '原子指标的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属业务板块的id',\n `data_domain_id` int(11) NOT NULL COMMENT '所属数据域的id',\n `biz_process_id` int(11) NOT NULL COMMENT '所属业务过程的id',\n `atom_index_name` varchar(128) NOT NULL COMMENT '原子指标英文名',\n `atom_index_name_cn` varchar(128) NOT NULL COMMENT '原子指标的中文名',\n `atom_index_comment` text COMMENT '原子指标的描述信息',\n `atom_index_data_type` varchar(32) NOT NULL COMMENT '原子指标的数据类型',\n `atom_index_unit` varchar(16) DEFAULT NULL COMMENT '原子指标的单位',\n `atom_index_creator` varchar(256) DEFAULT NULL COMMENT '原子指标的创建者',\n `atom_index_updater` varchar(256) DEFAULT NULL COMMENT '原子指标的最近更新者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '上次更新时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',\n `approve_opinion` varchar(1000) DEFAULT NULL COMMENT '审批意见',\n `approver` varchar(16) DEFAULT NULL COMMENT '最新一次的审核人',\n PRIMARY KEY (`atom_index_id`),\n KEY `idx_def_atom_index_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_atom_index_data_domain_id` (`data_domain_id`),\n KEY `idx_def_atom_index_biz_process_id` (`biz_process_id`),\n KEY `idx_def_atom_index_name_cn` (`atom_index_name_cn`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 2005702
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata数据同步－原子指标';


CREATE TABLE `onedata_biz_process`
(
    \n `biz_process_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '业务过程id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属的业务板块的id',\n `data_domain_id` int(11) NOT NULL COMMENT '所属数据域的id',\n `biz_process_name` varchar(32) NOT NULL COMMENT '业务过程的英文名',\n `biz_process_name_cn` varchar(128) NOT NULL COMMENT '业务过程的中文名',\n `biz_process_comment` text COMMENT '业务过程的描述信息',\n `biz_process_creator` varchar(16) DEFAULT NULL COMMENT '业务过程的创建者',\n `biz_process_updater` varchar(16) DEFAULT NULL COMMENT '业务过程的最近修改者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '最近一次修改时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',\n `approve_opinion` varchar(1000) DEFAULT NULL COMMENT '审批意见',\n `approver` varchar(16) DEFAULT NULL COMMENT '最新一次的审核人',\n PRIMARY KEY (`biz_process_id`),\n KEY `idx_def_biz_process_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_biz_process_data_domain_id` (`data_domain_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 20001425
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata规范定义－业务过程';


CREATE TABLE `onedata_data_domain`
(
    \n `data_domain_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '数据域的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属业务板块的id',\n `data_domain_name` varchar(32) NOT NULL COMMENT '数据域的英文名',\n `data_domain_name_cn` varchar(128) NOT NULL COMMENT '数据域的中文名',\n `data_domain_comment` text COMMENT '数据域的描述信息',\n `data_domain_owner` varchar(16) DEFAULT NULL COMMENT '数据域的owner，即为数据域的创建者和审核人',\n `data_domain_owner_bak` varchar(16) DEFAULT NULL COMMENT '数据域的备用owner',\n `gmt_created` datetime DEFAULT NULL COMMENT '数据域的创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '数据域的最近修改时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过，数据域只有审核通过的状态',\n `approver` varchar(16) DEFAULT NULL COMMENT '最新一次的审核人',\n `data_domain_creator` varchar(16) DEFAULT NULL COMMENT '数据域创建者',\n `data_domain_model_owner` varchar(64) DEFAULT NULL COMMENT '模型设计的owner',\n `data_domain_model_owner_bak` varchar(64) DEFAULT NULL COMMENT '模型设计的owner',\n PRIMARY KEY (`data_domain_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 20000442
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata规范定义－数据域';


CREATE TABLE `onedata_derived_index`
(
    \n `derived_index_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '派生指标的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属业务板块的id',\n `data_domain_id` int(11) NOT NULL COMMENT '所属数据域的id',\n `biz_process_id` int(11) NOT NULL COMMENT '所属业务过程的id',\n `atom_index_id` int(11) NOT NULL COMMENT '基本的原子指标的id',\n `time_period_id` int(11) NOT NULL DEFAULT '-1' COMMENT '派生指标使用的时间周期的id',\n `derived_index_name` varchar(128) NOT NULL COMMENT '派生指标的英文名',\n `derived_index_name_cn` varchar(128) NOT NULL COMMENT '派生指标的中文名',\n `derived_index_comment` text COMMENT '派生指标的描述信息',\n `derived_index_algorithm_desc` text COMMENT '派生指标的算法说明',\n `derived_index_data_type` varchar(32) DEFAULT NULL COMMENT '派生指标的数据类型',\n `derived_index_unit` varchar(16) DEFAULT NULL COMMENT '派生指标的单位',\n `derived_index_creator` varchar(256) DEFAULT NULL COMMENT '派生指标的创建者',\n `derived_index_updater` varchar(256) DEFAULT NULL COMMENT '派生指标的最近更新者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '最近更新时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',\n `approve_opinion` varchar(1000) DEFAULT NULL COMMENT '审批意见',\n `approver` varchar(16) DEFAULT NULL COMMENT '最新一次的审核人',\n PRIMARY KEY (`derived_index_id`),\n KEY `idx_def_derived_index_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_derived_index_biz_process_id` (`biz_process_id`),\n KEY `idx_def_derived_index_time_period_id` (`time_period_id`),\n KEY `idx_def_derived_index_suffix_index` (`time_period_id`,`biz_unit_id`,`atom_index_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 2040581
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata规范定义－派生指标';


CREATE TABLE `onedata_derived_index_adjunct_word_relation`
(
    \n `id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '自增id',\n `derived_index_id` int(11) NOT NULL COMMENT '派生指标id',\n `adjunct_word_id` int(11) NOT NULL COMMENT '修饰词id',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_def_derived_index_adjunct_word_relation_adjunct_word_uni` (`derived_index_id`,`adjunct_word_id`),\n KEY `idx_def_derived_index_adjunct_word_relation_adjunct_word_id` (`adjunct_word_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 116774
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata规范定义－修饰词和派生指标关系表';


CREATE TABLE `onedata_dimension`
(
    \n `dimension_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '维度的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属业务板块的id',\n `data_domain_id` int(11) NOT NULL COMMENT '所属数据域的id',\n `dimension_name` varchar(32) NOT NULL COMMENT '维度名',\n `dimension_name_cn` varchar(128) NOT NULL COMMENT '维度中文名',\n `dimension_comment` text COMMENT '维度的描述信息',\n `dimension_creator` varchar(16) DEFAULT NULL COMMENT '维度的创建者',\n `dimension_updater` varchar(16) DEFAULT NULL COMMENT '维度的上次更新者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '上次更新时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',\n `approve_opinion` varchar(1000) DEFAULT NULL COMMENT '审批意见',\n `approver` varchar(16) DEFAULT NULL COMMENT '最新一次的审核人',\n PRIMARY KEY (`dimension_id`),\n KEY `idx_def_dimension_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_dimension_data_domain_id` (`data_domain_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 1002114
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata规范定义－维度';


CREATE TABLE `onedata_dimension_attr`
(
    \n `dimension_attr_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '维度属性的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属业务板块的id',\n `data_domain_id` int(11) NOT NULL COMMENT '所属数据域的id',\n `dimension_id` int(11) NOT NULL COMMENT '所属的维度id',\n `dimension_attr_name` varchar(128) NOT NULL COMMENT '维度属性的英文名',\n `dimension_attr_name_cn` varchar(128) NOT NULL COMMENT '维度属性的中文名',\n `dimension_attr_comment` text COMMENT '维度属性的描述',\n `dimension_attr_data_type` varchar(32) NOT NULL COMMENT '维度属性的数据类型',\n `dimension_attr_creator` varchar(256) DEFAULT NULL COMMENT '维度属性的创建者',\n `dimension_attr_updater` varchar(256) DEFAULT NULL COMMENT '维度属性的上次更新者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '更新时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',\n `approve_opinion` varchar(1000) DEFAULT NULL COMMENT '审批意见',\n `approver` varchar(16) DEFAULT NULL COMMENT '最新一次的审核人',\n `dimension_attr_isindex` varchar(8) DEFAULT NULL COMMENT '是否是主键，默认为0不是，1为是',\n PRIMARY KEY (`dimension_attr_id`),\n KEY `idx_def_dimension_attr_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_dimension_attr_data_domain_id` (`data_domain_id`),\n KEY `idx_def_dimension_attr_dimension_id` (`dimension_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 1009377
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata规范定义－维度属性';


CREATE TABLE `onedata_time_period`
(
    \n `time_period_id` int
(
    11
) NOT NULL AUTO_INCREMENT COMMENT '时间周期的id',\n `biz_unit_id` int(11) NOT NULL COMMENT '所属的业务板块的id',\n `time_period_name` varchar(32) NOT NULL COMMENT '时间周期的英文名',\n `time_period_name_cn` varchar(128) NOT NULL COMMENT '时间周期的中文名',\n `time_period_comment` text COMMENT '时间周期的描述',\n `time_period_creator` varchar(256) DEFAULT NULL COMMENT '时间周期的创建者',\n `time_period_updater` varchar(256) DEFAULT NULL COMMENT '时间周期的修改者',\n `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '修改时间',\n `status` int(11) DEFAULT NULL COMMENT '状态字段，定义为1：草稿，2：待审核 3：审核通过，4.审核未通过',\n `approve_opinion` varchar(1000) DEFAULT NULL COMMENT '审核意见',\n `approver` varchar(16) DEFAULT NULL COMMENT '审核人',\n PRIMARY KEY (`time_period_id`),\n KEY `idx_def_time_period_biz_unit_id` (`biz_unit_id`),\n KEY `idx_def_time_period_name_cn` (`time_period_name_cn`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 4002645
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='onedata数据同步表-时间周期';


CREATE TABLE `se_activity_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_instance_id` bigint(20) unsigned DEFAULT NULL COMMENT 'process instance id',\n `process_definition_id_and_version` varchar(255) NOT NULL COMMENT 'process definition id and version',\n `process_definition_activity_id` varchar(64) NOT NULL COMMENT 'process definition activity id',\n PRIMARY KEY (`id`),\n KEY `idx_process_instance_id` (`process_instance_id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `se_default_id_generator`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 10000000
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine主键生成器';


CREATE TABLE `se_deployment_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_definition_id` varchar(255) NOT NULL COMMENT 'process definition id',\n `process_definition_version` varchar(255) DEFAULT NULL COMMENT 'process definition version',\n `process_definition_type` varchar(255) DEFAULT NULL COMMENT 'process definition type',\n `process_definition_code` varchar(255) DEFAULT NULL COMMENT 'process definition code',\n `process_definition_name` varchar(255) DEFAULT NULL COMMENT 'process definition name',\n `process_definition_desc` varchar(255) DEFAULT NULL COMMENT 'process definition desc',\n `process_definition_content` mediumtext NOT NULL COMMENT 'process definition content',\n `deployment_user_id` varchar(128) NOT NULL COMMENT 'deployment user id',\n `deployment_status` varchar(64) NOT NULL COMMENT 'deployment status',\n `logic_status` varchar(64) NOT NULL COMMENT 'logic status',\n PRIMARY KEY (`id`),\n KEY `idx_user_id_and_logic_status` (`logic_status`,`deployment_user_id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `se_execution_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_instance_id` bigint(20) unsigned NOT NULL COMMENT 'process instance id',\n `process_definition_id_and_version` varchar(255) NOT NULL COMMENT 'process definition id and version',\n `process_definition_activity_id` varchar(255) NOT NULL COMMENT 'process definition activity id',\n `activity_instance_id` bigint(20) unsigned NOT NULL COMMENT 'activity instance id',\n `active` tinyint(4) NOT NULL COMMENT '1:active 0:inactive',\n PRIMARY KEY (`id`),\n KEY `idx_process_instance_id_and_status` (`process_instance_id`,`active`),\n KEY `idx_process_instance_id_and_activity_instance_id` (`process_instance_id`,`activity_instance_id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `se_process_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_definition_id_and_version` varchar(128) NOT NULL COMMENT 'process definition id and version',\n `process_definition_type` varchar(255) DEFAULT NULL COMMENT 'process definition type',\n `status` varchar(64) NOT NULL COMMENT '1.running 2.completed 3.aborted',\n `parent_process_instance_id` bigint(20) unsigned DEFAULT NULL COMMENT 'parent process instance id',\n `parent_execution_instance_id` bigint(20) unsigned DEFAULT NULL COMMENT 'parent execution instance id',\n `start_user_id` varchar(128) DEFAULT NULL COMMENT 'start user id',\n `biz_unique_id` varchar(255) DEFAULT NULL COMMENT 'biz unique id',\n `reason` varchar(255) DEFAULT NULL COMMENT 'reason',\n `comment` varchar(255) DEFAULT NULL COMMENT 'comment',\n `title` varchar(255) DEFAULT NULL COMMENT 'title',\n `tag` varchar(255) DEFAULT NULL COMMENT 'tag',\n PRIMARY KEY (`id`),\n KEY `idx_start_user_id` (`start_user_id`),\n KEY `idx_status` (`status`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `se_task_assignee_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_instance_id` bigint(20) unsigned NOT NULL COMMENT 'process instance id',\n `task_instance_id` bigint(20) unsigned NOT NULL COMMENT 'task instance id',\n `assignee_id` varchar(255) NOT NULL COMMENT 'assignee id',\n `assignee_type` varchar(128) NOT NULL COMMENT 'assignee type',\n PRIMARY KEY (`id`),\n KEY `idx_task_instance_id` (`task_instance_id`),\n KEY `idx_assignee_id_and_type` (`assignee_id`,`assignee_type`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `se_task_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_instance_id` bigint(20) unsigned NOT NULL COMMENT 'process instance id',\n `process_definition_id_and_version` varchar(128) DEFAULT NULL COMMENT 'process definition id and version',\n `process_definition_type` varchar(255) DEFAULT NULL COMMENT 'process definition type',\n `activity_instance_id` bigint(20) unsigned NOT NULL COMMENT 'activity instance id',\n `process_definition_activity_id` varchar(255) NOT NULL COMMENT 'process definition activity id',\n `execution_instance_id` bigint(20) unsigned NOT NULL COMMENT 'execution instance id',\n `claim_user_id` varchar(255) DEFAULT NULL COMMENT 'claim user id',\n `title` varchar(255) DEFAULT NULL COMMENT 'title',\n `priority` int(11) DEFAULT '500' COMMENT 'priority',\n `tag` varchar(255) DEFAULT NULL COMMENT 'tag',\n `claim_time` datetime DEFAULT NULL COMMENT 'claim time',\n `complete_time` datetime DEFAULT NULL COMMENT 'complete time',\n `status` varchar(255) NOT NULL COMMENT 'status',\n `comment` varchar(255) DEFAULT NULL COMMENT 'comment',\n `extension` varchar(255) DEFAULT NULL COMMENT 'extension',\n PRIMARY KEY (`id`),\n KEY `idx_status` (`status`),\n KEY `idx_process_instance_id_and_status` (`process_instance_id`,`status`),\n KEY `idx_process_definition_type` (`process_definition_type`),\n KEY `idx_process_instance_id` (`process_instance_id`),\n KEY `idx_claim_user_id` (`claim_user_id`),\n KEY `idx_tag` (`tag`),\n KEY `idx_activity_instance_id` (`activity_instance_id`),\n KEY `idx_process_definition_activity_id` (`process_definition_activity_id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `se_variable_instance`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',\n `gmt_create` datetime NOT NULL COMMENT 'create time',\n `gmt_modified` datetime NOT NULL COMMENT 'modification time',\n `process_instance_id` bigint(20) unsigned NOT NULL COMMENT 'process instance id',\n `execution_instance_id` bigint(20) unsigned DEFAULT NULL COMMENT 'execution instance id',\n `field_key` varchar(128) NOT NULL COMMENT 'field key',\n `field_type` varchar(128) NOT NULL COMMENT 'field type',\n `field_double_value` decimal(65,30) DEFAULT NULL COMMENT 'field double value',\n `field_long_value` bigint(20) DEFAULT NULL COMMENT 'field long value',\n `field_string_value` varchar(4000) DEFAULT NULL COMMENT 'field string value',\n PRIMARY KEY (`id`),\n KEY `idx_process_instance_id_and_execution_instance_id` (`process_instance_id`,`execution_instance_id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='smartengine';


CREATE TABLE `t_deploy_process`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '修改时间',\n `commit_object_id` varchar(128) NOT NULL COMMENT '提交物id',\n `uuid` varchar(128) NOT NULL COMMENT 'uuid',\n `model_id` varchar(128) NOT NULL COMMENT '模型id',\n `model_type` varchar(64) NOT NULL COMMENT '模型类型',\n `agree_process_id` varchar(128) DEFAULT NULL COMMENT '审批流id',\n `workflow_process_id` varchar(128) NOT NULL COMMENT '工作流id',\n `big_engine_type` varchar(64) NOT NULL COMMENT '部署类型',\n `big_engine_ds` varchar(128) NOT NULL COMMENT '数据源',\n `release_runtime_env` varchar(64) NOT NULL COMMENT '部署环境',\n `status` varchar(64) NOT NULL COMMENT '流程状态',\n `business_unit_uuid` varchar(128) NOT NULL COMMENT '业务板块uuid',\n `business_process_name` varchar(128) DEFAULT NULL COMMENT '业务过程code',\n `model_code` varchar(128) DEFAULT NULL COMMENT '模型code',\n `tenant_id` varchar(64) NOT NULL COMMENT '租户id',\n `project_id` varchar(64) NOT NULL COMMENT '项目id',\n `releaser_id` varchar(64) NOT NULL COMMENT '发布者id',\n `releaser_name` varchar(64) NOT NULL COMMENT '发布者名称',\n `release_date` datetime NOT NULL COMMENT '发布时间',\n `release_remark` text NOT NULL COMMENT '发布备注',\n `creator_id` varchar(128) NOT NULL COMMENT '创建人租户baseId',\n `creator_name` varchar(128) NOT NULL COMMENT '创建人显示名称',\n `modifier_id` varchar(128) DEFAULT NULL COMMENT '修改人租户baseid',\n `modifier_name` varchar(128) DEFAULT NULL COMMENT '修改人显示名称',\n `version` bigint(20) NOT NULL COMMENT '版本号',\n `big_engine_dialect_type` varchar(64) NOT NULL COMMENT '大数据dsl方言类型',\n `resource_group_id` varchar(128) DEFAULT NULL COMMENT '调度独立资源组id',\n `deploy_mode` varchar(64) DEFAULT NULL COMMENT '部署模式',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='部署过程表';


CREATE TABLE `t_draft_object`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '修改时间',\n `committer_id` varchar(64) NOT NULL COMMENT '提交人id',\n `committer_name` varchar(64) DEFAULT NULL COMMENT '提交人姓名',\n `commit_remark` text NOT NULL COMMENT '提交备注',\n `fml_statement` text NOT NULL COMMENT 'fml语句',\n `business_unit_uuid` varchar(64) NOT NULL COMMENT '所属的业务板块uuid',\n `tenant_id` varchar(128) NOT NULL COMMENT '租户id',\n `project_id` varchar(128) NOT NULL COMMENT '项目id',\n `creator_id` varchar(64) NOT NULL COMMENT '创建人租户baseId',\n `creator_name` varchar(64) DEFAULT NULL COMMENT '创建人显示名称',\n `modifier_id` varchar(64) DEFAULT NULL COMMENT '修改人租户baseId',\n `modifier_name` varchar(64) DEFAULT NULL COMMENT '修改人显示名称',\n `version` int(11) NOT NULL COMMENT '版本号',\n `commit_date` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '提交时间',\n `model_id` varchar(128) NOT NULL COMMENT '模型id',\n `model_type` varchar(128) NOT NULL COMMENT '模型类型',\n `business_process_name` varchar(70) DEFAULT NULL COMMENT '业务过程名称',\n `uuid` varchar(128) NOT NULL COMMENT '提交对象唯一id，替代id用于迁移方便，符合团队规范',\n `previous_draft_object_id` varchar(128) DEFAULT NULL COMMENT '上一次保存草稿对象的uuid',\n `model_version` int(11) NOT NULL DEFAULT '0' COMMENT '模型版本',\n `model_code` varchar(128) DEFAULT NULL COMMENT '模型code',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_uuid_idx` (`uuid`),\n KEY `idx_model` (`model_id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 23
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='草稿对象表';


CREATE TABLE `t_stage_object`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime DEFAULT NULL COMMENT '修改时间',\n `uuid` varchar(128) NOT NULL COMMENT '暂存对象唯一id，替代id用于迁移方便，符合团队规范',\n `model_id` varchar(128) NOT NULL COMMENT '模型id',\n `model_type` varchar(128) NOT NULL COMMENT '模型类型',\n `business_process_name` varchar(70) DEFAULT NULL COMMENT '业务过程名称',\n `commit_object_id` varchar(128) NOT NULL COMMENT '提交物对象id',\n `model_code` varchar(128) DEFAULT NULL COMMENT '模型code',\n `project_id` varchar(128) DEFAULT NULL COMMENT '项目id',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_uuid_idx` (`uuid`),\n KEY `idx_model` (`model_id`,`model_type`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='暂存对象表';


CREATE TABLE `table_source`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `table_uuid` varchar(64) NOT NULL COMMENT '表uuid',\n `column_uuid` varchar(64) DEFAULT NULL COMMENT '表字段uuid',\n `source_project_name` varchar(128) NOT NULL COMMENT '来源项目',\n `source_table_code` varchar(128) NOT NULL COMMENT '来源表编码',\n `source_column_code` varchar(128) DEFAULT NULL COMMENT '来源字段编码',\n `table_source_relation_uuid` varchar(64) DEFAULT NULL COMMENT '关联来源关系UUID',\n `business_unit_uuid` varchar(64) NOT NULL COMMENT '业务板块uuid',\n `tenant_id` varchar(64) NOT NULL COMMENT '租户ID',\n `creator_id` varchar(128) NOT NULL COMMENT '创建人baseID',\n `creator_name` varchar(128) NOT NULL COMMENT '创建人名称',\n `modifier_id` varchar(128) NOT NULL COMMENT '修改人baseID',\n `modifier_name` varchar(128) NOT NULL COMMENT '修改人名称',\n `is_deleted` tinyint(4) NOT NULL COMMENT '是否删除',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='表来源表';


CREATE TABLE `table_source_relation`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `uuid` varchar(64) NOT NULL COMMENT '来源关系唯一标识',\n `table_uuid` varchar(64) NOT NULL COMMENT '表uuid',\n `column_uuid` varchar(64) DEFAULT NULL COMMENT '字段uuid',\n `right_source_project_name` varchar(128) DEFAULT NULL COMMENT '右表项目',\n `right_source_table_code` varchar(128) DEFAULT NULL COMMENT '右表编码',\n `right_source_column_code` varchar(128) DEFAULT NULL COMMENT '右表字段编码',\n `creator_id` varchar(128) NOT NULL COMMENT '创建人baseID',\n `creator_name` varchar(128) NOT NULL COMMENT '创建人名称',\n `modifier_id` varchar(128) NOT NULL COMMENT '修改人baseID',\n `modifier_name` varchar(128) NOT NULL COMMENT '修改人名称',\n `is_deleted` tinyint(4) NOT NULL COMMENT '是否删除',\n `business_unit_uuid` varchar(64) NOT NULL COMMENT '业务板块uuid',\n `tenant_id` varchar(64) NOT NULL COMMENT '租户ID',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='表来源关系表';


CREATE TABLE `tag`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `tenant_id` varchar(128) DEFAULT NULL COMMENT '租户id',\n `creator_id` varchar(64) DEFAULT NULL COMMENT '创建人租户baseId',\n `creator_name` varchar(64) DEFAULT NULL COMMENT '创建人显示名称',\n `uuid` varchar(128) DEFAULT NULL COMMENT '全局唯一的uuid',\n `business_unit_uuid` varchar(64) DEFAULT NULL COMMENT '业务板块uuid',\n `name` varchar(64) DEFAULT NULL COMMENT '标签名称',\n `tag_type` tinyint(4) DEFAULT NULL COMMENT '标签类型',\n `source_type` tinyint(4) DEFAULT NULL COMMENT '来源类型',\n PRIMARY KEY (`id`),\n UNIQUE KEY `uk_tag_uuid` (`uuid`),\n UNIQUE KEY `uk_tag_name` (`tenant_id`,`business_unit_uuid`,`tag_type`,`name`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='通用标签表';


CREATE TABLE `tag_rel_entity`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `tenant_id` varchar(128) DEFAULT NULL COMMENT '租户id',\n `creator_id` varchar(64) DEFAULT NULL COMMENT '创建人租户baseId',\n `creator_name` varchar(64) DEFAULT NULL COMMENT '创建人显示名称',\n `business_unit_uuid` varchar(64) DEFAULT NULL COMMENT '业务板块uuid',\n `tag_uuid` varchar(64) DEFAULT NULL COMMENT '标签uuid',\n `entity_uuid` varchar(64) DEFAULT NULL COMMENT '实体uuid',\n PRIMARY KEY (`id`),\n KEY `idx_tag_rel_entity_uuid` (`entity_uuid`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='标签和实体的关联关系';


CREATE TABLE `task`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n `tenant_id` varchar(64) NOT NULL COMMENT '租户id',\n `project_id` varchar(64) NOT NULL COMMENT '当前项目空间id',\n `source_project_id` varchar(64) NOT NULL COMMENT '来源项目空间id',\n `creator_id` varchar(128) NOT NULL COMMENT '创建者id',\n `creator_name` varchar(128) NOT NULL COMMENT '创建者名称',\n `description` varchar(2048) DEFAULT NULL COMMENT '描述',\n `is_deleted` tinyint(4) NOT NULL COMMENT '是否删除',\n `uuid` varchar(64) NOT NULL COMMENT '任务编码',\n `type` varchar(32) NOT NULL COMMENT '前缀方式：PREFIX,精确表名：CODE',\n `content` text NOT NULL COMMENT '内容，表名或者前缀列表',\n `pattern` varchar(4096) NOT NULL COMMENT '解析规则',\n `reverse_mode` varchar(32) NOT NULL COMMENT '全量：DELETE_BEFORE_INSERT \\n增量：INSERT_IF_NOT_EXIST',\n `status` varchar(32) NOT NULL COMMENT '未开始：CREATED\\n进行中：RUNNING\\n完成：FINISHED',\n `name` varchar(128) NOT NULL COMMENT '任务名称',\n `total` int(10) unsigned DEFAULT NULL COMMENT '任务明细数量',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    AUTO_INCREMENT = 11
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='逆向任务表';


CREATE TABLE `task_detail`
(
    \n `id` bigint
(
    20
) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n `gmt_create` datetime NOT NULL COMMENT '创建时间',\n `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n `command` text NOT NULL COMMENT '冗余的执行参数',\n `dm_context` varchar(2048) DEFAULT NULL COMMENT '模型上下文',\n `type` tinyint(4) NOT NULL COMMENT '模型类型，维度表、事实表、汇总表',\n `table_meta` text COMMENT '表元仓数据',\n `result` text COMMENT '执行结果',\n `status` varchar(32) NOT NULL COMMENT '任务明细执行状态',\n `task_id` varchar(64) NOT NULL COMMENT '关联的任务uuid',\n `modifier_id` varchar(128) NOT NULL COMMENT '修改人id',\n `modifier_name` varchar(128) NOT NULL COMMENT '修改人名称',\n `is_deleted` tinyint(4) NOT NULL COMMENT '是否删除',\n `uuid` varchar(64) NOT NULL COMMENT '主键',\n PRIMARY KEY (`id`)\n)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci COMMENT ='任务明细表';


