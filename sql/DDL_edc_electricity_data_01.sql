USE [IEA_External-DB]
GO

/****** Object:  Table [edc].[electricity_data]    Script Date: 02-12-2020 10:27:55 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE SCHEMA [edc]
GO

CREATE TABLE [edc].[electricity_data](
	[utc_datetime] [datetime] NULL,
	[utc_date] [date] NULL,
	[value] [bigint] NULL,
	[country] [varchar](max) NULL,
	[source] [varchar](max) NULL,
	[export date] [datetime] NULL,
	[flow 2] [varchar](max) NULL,
	[metric] [varchar](max) NULL,
	[product] [varchar](max) NULL,
	[flow 1] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

GRANT SELECT ON [edc].[electricity_data] TO [IEA_EXTERNAL-DB_READ]
GO