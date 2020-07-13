# CN-DBpedia-To-Neo4j
将[CN-DBpedia](http://kw.fudan.edu.cn/cndbpedia/intro/) 利用neo4j-admin导入neo4j。

首先使用code/txt2csv将CN-DBpedia的txt数据文件转换成neo4j-admin需要的两个csv文件，
具体原理参考[这个教程](https://www.jianshu.com/p/3acbf66bd0d0) 。

后续导入步骤我写在CSDN里了，[【neo4j】win10上利用neo4j-admin导入csv](https://blog.csdn.net/weixin_41185456/article/details/107313250) 。我只在win10上导入了数据，其他平台和其他版本的问题请自行解决。