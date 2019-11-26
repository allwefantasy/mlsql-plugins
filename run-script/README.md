## Install 

```sql
!plugin et add tech.mlsql.plugins.et.RunScript "run-script" named runScript;
```

## Usage

```sql
set code1='''
select 1 as a as b;
''';
!runScript '''${code1}''' named output;
```