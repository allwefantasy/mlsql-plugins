## Install 

```sql
!plugin et add - "table-repartition-2.4";
```

## Usage

```sql
set rawText='''
{"id":9,"content":"Spark好的语言1","label":0.0}
{"id":10,"content":"MLSQL是一个好的语言7","label":0.0}
{"id":13,"content":"MLSQL是一个好的语言7","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

select id,content,label from orginal_text_corpus as orginal_text_corpus1;
run orginal_text_corpus1 as TableRepartition.`` where partitionNum="2" and partitionType="range" and partitionCols="id"
as newtable;
```