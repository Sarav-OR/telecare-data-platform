/* Run as the Entra admin of the server (Query editor, Microsoft Entra sign-in),
   AFTER 01_create_ehr_schema.sql and ../ctl/03_control_tables.sql.

   Least privilege for the Data Factory managed identity:
     ehr.*  read only                      (it is a source system: ADF never writes there)
     ctl.*  read + execute the procedures  (watermarks, batch state and run log are
                                            changed ONLY through the procedures)       */
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'adf-telecare-dev-chn')
    CREATE USER [adf-telecare-dev-chn] FROM EXTERNAL PROVIDER;
GO
GRANT SELECT ON SCHEMA::ehr TO [adf-telecare-dev-chn];
GRANT SELECT ON SCHEMA::ctl TO [adf-telecare-dev-chn];
GRANT EXECUTE ON SCHEMA::ctl TO [adf-telecare-dev-chn];
GO
