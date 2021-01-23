Program is managed via the config.ini

You can add tables, emails (sends config at end of run), and update database connections

BULK_TABLES
add smaller tables to this section in the config.
This will truncate the table in mssql then migrate all avialable data from postgres to the table.
uf a bulk update is taking way too long consider moving your table to a DELTA_MERGE

DELTA_MERGE
add large tables to this section.
This truncate the MSSQL delta table and get data based on the interval from postgres then merge it to the main table in MSSQL.


INTERVAL
update this section to specify how far you want to pull data back from postgres in the DELTA_MERGE

EMAILS
update this section to specify who gets notifyed at the end of a run

POSTGRESQL
update this section to specify the postgres server and default DB to connect to.
update password and username here as well


MSSQL
update this section to specify the MSSQL server and default DB to connect to.
update password and username here as well