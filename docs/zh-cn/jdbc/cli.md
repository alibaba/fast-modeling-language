# Cli

FML支持通过Cli(command line interface)访问建模引擎。 


## 安装 

下载fast-modeling-language源码，在根目录下执行
```shell
mvn clean install -Dmaven.test.skip 
```

```
cd fastmodel-driver/fastmodel-driver-cli/target/zip
```

## 配置

```
unzip fastmodel-driver-cli-${version}-distribution.zip
```

```shell
fastmodel.url=localhost:18080
fastmodel.ssl=false
fastmodel.database=
fastmodel.password=
fastmodel.user=
```


## 启动

```shell
./start.sh
```







