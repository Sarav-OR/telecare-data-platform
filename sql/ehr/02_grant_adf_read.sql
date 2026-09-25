/* Run as the Entra admin of the server (Query editor, Microsoft Entra sign-in).
   Gives the Data Factory managed identity read access to the EHR schema only. */
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'adf-telecare-dev-chn')
    CREATE USER [adf-telecare-dev-chn] FROM EXTERNAL PROVIDER;
GO
GRANT SELECT ON SCHEMA::ehr TO [adf-telecare-dev-chn];
GO
