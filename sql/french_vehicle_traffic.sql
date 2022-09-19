USE [external_db_dev]
GO

/****** Object:  Table [main].[french_vehicle_traffic]    Script Date: 03-08-2021 19:05:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [main].[french_vehicle_traffic](
	[Zone] [varchar](max) NULL,
	[date] [datetime] NULL,
	[ITV] [float] NULL,
	[IPL] [float] NULL,
	[MGL_ITV] [float] NULL,
	[MGL_IPL] [float] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

GRANT SELECT ON[main].[french_vehicle_traffic] TO [IEA_EXTERNAL-DB_READ]
GO