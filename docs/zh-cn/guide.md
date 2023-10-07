本文将从一个现实中实际的建模的例子出发，展示如何使用FML进行建模和指标建设的过程。


# 需求背景
业务需求：
在教育行业下，在线教育是最近几年突飞猛进的行业。 对老师的上课计划以及评估上课效果的需求是基本的需求之一。 本文从一个数据分析者的角度出发，需要对现有教学情况有一个基本情况的摸底，涉及的需求如下：


- 当前学期全部学生排课次数
- 当前学期全部学生出席次数
- 当前学期学生旷课缺席次数
- 当前学期学生病假缺席次数



在上面的场景下，先定义数仓规划的操作，涉及概念如下：

| 概念 | 英文单词 | 说明 |
| --- | --- | --- |
| 业务板块 | business_unit | 描述业务域的信息，比如：教育板块 |
| 数据域 | domain | 定义业务域下，具体的数据范围。比如：会员域 |
| 业务过程 | business_process | 业务过程指企业的业务活动事件，如上课 |
| 数仓分层 | layer | 对数仓中的数据进行顶层设计。 |
| 指标 | indicator | 衡量目标的方法，预期中打算达到的指数、规格、标准，一般用数字表示，具备全局唯一的标准化特性 |
| 时间周期 | time_period | 定义时间范围 |
| 维度表 | dim table | 定义维度信息 |
| 事实表 | fact table | 定义事实发生明细信息数据 |
| 修饰词 | Adjunct | 修饰词是对指标统计业务范围的划定，指除了统计维度外指标的业务场景的限定抽象，如PC端，无线端 |



# 定义业务板块
业务板块一般是由系统自动创建，以本文需求为例，系统会自动创建一个名为：edu —— 教育行业的业务板块。


# 定义数据域
一个业务板块下可以挂靠多个数据域，数据域是一个或多个业务过程的集合。
可以通过FML进行数据域的定义，一个数据域在一个板块下是唯一定义的。
```fml
CREATE DOMAIN IF NOT EXSITS edu.dm_student COMMENT '学生管理'; 
```


# 定义业务过程
我们可以通过FML定义指定的业务过程，业务过程是描述业务发生的过程内容。同样在一个数据板块下，只存在同一个业务过程。
我们可以将多个业务过程关联在一个数据域下。 下面的命令是创建一个学生上课的业务过程，同时将它挂在学生的数据域下。
```fml
CREATE BUSINESS_PROCESS IF NOT EXISTS edu.student_attend_class COMMENT '支付过程' WITH ('doamin_key'='dm_student');
```


# 定义数仓分层
数仓分层是数仓建设的顶层设计，是基于数据的角度出发，将数仓中模型信息进行分层划分，下面是通过FML语句创建了常用的三个分层架构： DIM层，公共维度模型层，DWD明细数据层， DWS汇总数据层。
```fml

CREATE LAYER IF NOT EXISTS edu.DIM COMMENT '公共维度模型层';

CREATE LAYER IF NOT EXISTS edu.DWD COMMENT '明细数据层';

CREATE LAYER IF NOT EXISTS edu.DWS COMMENT '汇总数据层';


```


# 维度建模
维度建模的过程一般分为几步：

- 定义业务过程
- 识别粒度
- 定义维度
- 定义事实

业务过程已经在上面按照FML定义完成了，所以我们接下来使用FML定义维度和事实


## 定义维度表
接下来我们根据业务需求，来首先建立学生维度表：
```fml
CREATE DIM TABLE IF NOT EXISTS edu.dim_student
(
  student_no string COMMENT '学号',
  student_name string COMMENT '姓名',
  gender string COMMENT '性别（0：女，1：男）',
  age bigint COMMENT '年龄',
  address string COMMENT '家庭住址',
  tel_no string COMMENT '手机号码',
  gmt_term datetime COMMENT '学期开始时间',
  primary key (student_no)
) COMMENT '学生' WITH ('business_process'='student_attend_class', 'data_layer'DIM');
```




## 定义事实表
事实表是表达具体发生的业务过程，关于事实表的关键，是识别粒度表达：
```fml

-- 事实表-学生上课
CREATE FACT TABLE IF NOT EXISTS edu.fact_student_clock_in_class
(
  student_no string COMMENT '学号',
  school_timetable_code string COMMENT '课程表编码',
  course_code string COMMENT '课程编码',
  absent_tag bigint COMMENT '是否缺席(0:否,1:是)',
  absent_type string COMMENT '缺席类型，比如旷课、病假等等',
  gmt_create datetime COMMENT '上课日期',
  gmt_clock_in_class_time datetime COMMENT '上课打卡时间',
  gmt_clock_out_after_class_time datetime COMMENT '下课打卡时间',
  in_class_minutes datetime COMMENT '上课总时长(单位：分钟)',
  primary key (student_no, school_timetable_code),
  constraint fact_student_clock_in_class_rel_dim_student DIM KEY (student_no) REFERENCES dim_student(student_no)
) COMMENT '事实-学生上课' WITH PROPERTIES('business_process'='student_attend_class', 'data_layer'='DWD');

```


# 指标
当我们定义完维度和事实表之后，就可以根据事实表和维度表的信息，定义指标信息。
按照指标类型分为原子指标、派生指标，根据上面的业务需求，


## 原子指标
```fml
-- 创建原子指标-上课出勤次数
CREATE ATOMIC INDICATOR edu.clock_in_class_cnt bigint COMMENT '上课出勤次数'
WITH (
 'data_unit' = 'ci'
,'is_distinct' = 'true'
,'agg_function' = 'count'
,'extend_name' = 'clock_in_class_cnt'
,'business_process' = 'student_attend_class'
,'biz_caliber' = '学生上课出勤次数'
);

```


## 派生指标
派生指标是由时间周期、修饰词、原子指标组成。


### 时间周期
创建时间周期，定义一个学年的时间周期
```fml
-- 创建时间周期-now_term_year
CREATE TIME_PERIOD edu.now_term_year COMMENT '当前学年'
WITH PROPERTIES(
'type' = 'YEAR',
'extend_name' = 'now_term_year'
)
AS BETWEEN TO_BEGIN_DATE_WITH_FIRST_DAY('${bizdate}', 'y', 0, 0) AND TO_END_DATE_WITH_FIRST_DAY('${bizdate}', 'y', 0, 0);
```

### 修饰词
创建，男学生、女学生、全部学生的修饰词定义。
```fml
-- 修饰词-男学生
CREATE ADJUNCT edu.student_gender_boy comment '男学生'
WITH (
 'extend_name' = 'student_gender_boy'
,'biz_caliber'='男学生'
)
AS dim_student.gender = '1';

-- 修饰词-女学生
CREATE ADJUNCT edu.student_gender_girl comment '女学生'
WITH (
 'extend_name' = 'student_gender_girl'
,'biz_caliber'='女学生'
)
AS dim_student.gender = '0';

-- 修饰词-全部
CREATE ADJUNCT edu.all_tag comment '全部'
WITH (
 'extend_name' = 'all_tag'
,'biz_caliber'='全部'
,'type' = 'NONE'
)
```
### 派生指标
我们可以通过批量的方式，创建派生指标，而不需要一个个定义。
```fml
-- 批量创建派生指标-学生考勤
create batch edu.batch_code (
  need_clock_in_class_cnt_0000 comment '当前学期_学生_全部_排课次数' references need_clock_in_class_cnt as count(1)
, clock_in_class_cnt_0000 comment '当前学期_学生_全部_出席次数' references clock_in_class_cnt as count(case when fact_student_clock_in_class.absent_tag = 0 then 1 else null end)
, not_clock_in_class_cnt_0000 comment '当前学期_学生_旷课_缺席次数' adjunct(kuangke_tag) references not_clock_in_class_cnt as count(1)
, not_clock_in_class_cnt_0001 comment '当前学期_学生_病假_缺席次数' adjunct(bingjia_tag) references not_clock_in_class_cnt as count(1)
, time_period now_term_year
, from table (fact_student_clock_in_class)
, date_field (gmt_create, 'yyyy-MM-dd')
, adjunct (all_tag)
, dim table (dim_student)
) with ('is_async'='false');
```


# 总结
支持我们完成从需求定义，到使用FML语法的创建处理。
