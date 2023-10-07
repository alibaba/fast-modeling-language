| 类型              | 功能                     |
|-----------------|------------------------|
| show references | 来查看对象的依赖的内容 | 

- 使用场景

    - 查看业务过程的引用列表
    - 查看数据域的引用列表

- 命令格式

```
KW_SHOW <showType> KW_REFERENCES KW_FROM <from> (KW_WITH <properties>)?

```

- 参数说明
    - showType : 必选。详细可以查看
    - from : 必选，引用所属的对象名称
    - properties : 可选，key=value的形式，需要用单引号起来
- 示例
    - 示例1，查看数据域为tmall的依赖内容

```
  show domain references from tmall
```




