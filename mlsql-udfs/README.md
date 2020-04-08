## Install

```
!plugin app add - "mlsql-udf";
```

## Usage

Date UDF:

```sql
select parseDateAsLong("2017-12-20","yyyy-MM-dd") as dateLong as temp1;
select parseLongAsDate(dateLong,"yyyy:MM:dd") as dateStr as temp2;
```





