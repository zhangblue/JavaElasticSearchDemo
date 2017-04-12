# JavaElasticSearchDemo

## ElasticSearch使用2.3.3版本 Java采用1.8版本JDK

### 主要功能代码说明：
#### 1.将数据逐条插入到elasticsearch中
```
JavaToES.insertES()
```
---
#### 2.将elasticsearch中的index备份到本地文件
```apple js
@param _index 要备份的index名字
@param _type 要备份的type名字
@param filePath 备份到本地的文件路径
backUpElasticsearchToLocalFile(String _index, String _type, String filePath)

```
---

#### 3.将备份的文件恢复到elasticsearch中
```
@param filePath 本地文件名
@param fileLineNumber 文件行数
restoreElasticsearchFromLocalFile(long fileLineNumber, String filePath)
```
---
#### 4.得到elasticsearch中的所有index名称
```
backUpElasticSearchIndex()
```
---
