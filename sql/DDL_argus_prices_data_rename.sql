EXEC sp_RENAME '[argus_prices].[argus_prices_data].[Module]', 'Folder', 'COLUMN'
GO

alter table [argus_prices].[argus_prices_data]
add FileName varchar(20);

GO

EXEC sp_RENAME '[argus_prices].[forward_curves_data].[Module]', 'Folder', 'COLUMN'
GO

alter table [argus_prices].[forward_curves_data]
add FileName varchar(20);

GO