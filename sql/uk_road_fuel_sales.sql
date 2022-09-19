/****** Object:  Table [main].[uk_road_fuel_sales]    Script Date: 05-08-2021 17:08:15 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [main].[uk_road_fuel_sales_data](
	[weekday] [varchar](9) NULL,
	[fuel_type] [varchar](6) NULL,
	[region] [varchar](24) NULL,
	[sales_litres] [bigint] NULL,
	[date_created] [datetime] null,
	[date_modified] [datetime] null
)
GO

GRANT SELECT ON[main].[uk_road_fuel_sales_data] TO [IEA_EXTERNAL-DB_READ]
GO