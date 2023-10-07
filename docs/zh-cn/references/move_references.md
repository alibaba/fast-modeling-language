## Move References

| 类型              | 功能      |
|-----------------|---------|
| move references | 移动对象的引用 | 

一般配合`show REFERENCES`进行使用,将原来的对象移动到其他对象里，比如一个维度表原本是挂靠在数据域A上，希望可以挂靠在数据域B上。一种可以通过设置表的属性进行切换， 但是当数据域里的表比较多时，需要将 但是使用上比较低效，这时可以使用`move` 将数据域A移动到数据域B中， 命令：

- 命令格式

```
 MOVE  <object_type> REFERENCES FROM <source> TO <target> [WITH <properties>]
```

- 限制条件
    - source和target如果一样，将不会任何内容处理
- 命令参数
    - object_type : 对象类型，必选
        - 支持类型：
    - source ： 必选，标识符, 可以使用：a.b.c这种形式
    - target ： 必选，标识符,可以使用：a.b.c这种形式

- 示例1， 将数据域tmall下引用的内容，都移动到数据域淘宝

```plain
    move data_domain references from tmall to tb 
```




