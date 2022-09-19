CREATE TABLE [kpler].[russian_exports_data](
	[date] [date] NULL,
	[product] [varchar](30) NULL,
	[origin_installation] [varchar](60) NULL,
	[seller] [varchar](60) NULL,
	[value] [float] NULL,
	[date_created] [datetime] NULL,
	[date_modified] [datetime] NULL
)
GO

create view [main].[V_KPLER_RUSSIAN_EXPORTS_DATA]
as
-- This view calculates the Russian exports based on Kpler data (currently only for Crude/Condensates)
-- It gets the total for each terminal, considering the exceptions below:
--
-- - CPC Terminal: consider only seller 'Lukoil'
-- - Sheskharis: remove Kazakh oil (KMG, SOCAR)
-- - Ust Luga: remove Kazakh oil (KMG)
select [date], [product], origin_installation,  SUM([value]) as value
from [kpler].[russian_exports_data]
where
    (origin_installation not in ('CPC Terminal', 'Sheskharis', 'TNTK Crude Ust Luga', 'Ust Luga Oil Terminal') and seller = 'TOTAL')
	or
	(origin_installation = 'CPC Terminal' and seller = 'Lukoil')
	or
	(origin_installation = 'Sheskharis' and seller not in ('KMG', 'SOCAR', 'TOTAL'))
	or
	(origin_installation = 'Ust Luga' and seller not in ('KMG', 'TOTAL'))
group by [date], [product], [origin_installation]

GRANT SELECT ON [main].[V_KPLER_RUSSIAN_EXPORTS_DATA] TO [IEA_EXTERNAL-DB_READ]
GO