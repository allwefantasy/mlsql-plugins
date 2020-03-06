## Install 

```sql
!plugin et add - "run-script" named runScript;
```

## Usage

```sql
set code1='''
select 1 as a as b;
''';
!runScript '''${code1}''' named output;
```