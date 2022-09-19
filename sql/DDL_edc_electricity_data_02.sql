USE [IEA_External-DB]
GO

/****** Object:  Table [edc].[electricity_data]    Script Date: 02-12-2020 10:34:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER TABLE [edc].[electricity_data]
ADD
	[local_datetime] [datetime] NULL,
	[local_date] [date] NULL,
    [Region]       varchar(max) NULL,
    [Flow 3]       varchar(max) NULL,
    [Flow 4]       varchar(max) NULL;

GO
