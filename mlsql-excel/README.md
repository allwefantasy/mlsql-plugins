## Install

```
!plugin ds add - "mlsql-excel-2.4";
```

or install as app:

```
tech.mlsql.plugins.ds.MLSQLApp
```


## Usage

```sql
load excel.`/tmp/upload/example_en.xlsx` 
where useHeader="true" and 
maxRowsInMemory="100" 
and dataAddress="A1:C8"
as data;

select * from data as output;
```





