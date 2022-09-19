/****** Object:  Table [main].[paj_jp_oil_stats_weekly_data]    Script Date: 11-12-2020 21:08:53 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--drop table [main].[paj_jp_oil_stats_weekly_data]
--go

CREATE TABLE [main].[paj_jp_oil_stats_weekly_data](
	[Current Week] [varchar](23) NULL,
	[Refinery Operations - Crude Input(kl)] [float] NULL,
	[Refinery Operations - Weekly Average Capacity(BPSD)] [float] NULL,
	[Refinery Operations - Util. Rate against BPSD] [float] NULL,
	[Refinery Operations - Designed Capacity(BPCD)] [float] NULL,
	[Refinery Operations - Util. Rate against BPCD] [float] NULL,
	[Products Stocks(kl) - Crude Oil] [float] NULL,
	[Products Stocks(kl) - Gasoline] [float] NULL,
	[Products Stocks(kl) - Naphtha] [float] NULL,
	[Products Stocks(kl) - Jet] [float] NULL,
	[Products Stocks(kl) - Kerosene] [float] NULL,
	[Products Stocks(kl) - Gas Oil(Diesel)] [float] NULL,
	[Products Stocks(kl) - LSA] [float] NULL,
	[Products Stocks(kl) - HSA] [float] NULL,
	[Products Stocks(kl) - AFO] [float] NULL,
	[Products Stocks(kl) - LSC] [float] NULL,
	[Products Stocks(kl) - HSC] [float] NULL,
	[Products Stocks(kl) - CFO] [float] NULL,
	[Products Stocks(kl) - Total] [float] NULL,
	[Unfinished Oil Stocks(kl) - Unfinished Gasoline] [float] NULL,
	[Unfinished Oil Stocks(kl) - Unfinished Kerosene] [float] NULL,
	[Unfinished Oil Stocks(kl) - Unfinished Gas Oil] [float] NULL,
	[Unfinished Oil Stocks(kl) - Unfinished AFO] [float] NULL,
	[Unfinished Oil Stocks(kl) - Feed Stocks] [float] NULL,
	[Unfinished Oil Stocks(kl) - Total] [float] NULL,
	[Refinery Production(kl) - Gasoline] [float] NULL,
	[Refinery Production(kl) - Naphtha] [float] NULL,
	[Refinery Production(kl) - Jet] [float] NULL,
	[Refinery Production(kl) - Kerosene] [float] NULL,
	[Refinery Production(kl) - Gas Oil(Diesel)] [float] NULL,
	[Refinery Production(kl) - LSA] [float] NULL,
	[Refinery Production(kl) - HSA] [float] NULL,
	[Refinery Production(kl) - AFO] [float] NULL,
	[Refinery Production(kl) - LSC] [float] NULL,
	[Refinery Production(kl) - HSC] [float] NULL,
	[Refinery Production(kl) - CFO] [float] NULL,
	[Refinery Production(kl) - Total] [float] NULL,
	[Imports(kl) - Gasoline] [float] NULL,
	[Imports(kl) - Naphtha] [float] NULL,
	[Imports(kl) - Jet] [float] NULL,
	[Imports(kl) - Kerosene] [float] NULL,
	[Imports(kl) - Gas Oil(Diesel)] [float] NULL,
	[Imports(kl) - LSA] [float] NULL,
	[Imports(kl) - HSA] [float] NULL,
	[Imports(kl) - AFO] [float] NULL,
	[Imports(kl) - LSC] [float] NULL,
	[Imports(kl) - HSC] [float] NULL,
	[Imports(kl) - CFO] [float] NULL,
	[Imports(kl) - Total] [float] NULL,
	[Exports(kl) - Gasoline] [float] NULL,
	[Exports(kl) - Naphtha] [float] NULL,
	[Exports(kl) - Jet] [float] NULL,
	[Exports(kl) - Kerosene] [float] NULL,
	[Exports(kl) - Gas Oil(Diesel)] [float] NULL,
	[Exports(kl) - LSA] [float] NULL,
	[Exports(kl) - HSA] [float] NULL,
	[Exports(kl) - AFO] [float] NULL,
	[Exports(kl) - LSC] [float] NULL,
	[Exports(kl) - HSC] [float] NULL,
	[Exports(kl) - CFO] [float] NULL,
	[Exports(kl) - Total] [float] NULL,
	[First Day of Week] [datetime] NOT NULL,
	[date_created] [datetime] NULL,
	[date_modified] [datetime] NULL
) ON [PRIMARY]
GO

ALTER TABLE [main].[paj_jp_oil_stats_weekly_data]
   ADD CONSTRAINT PK_fdw PRIMARY KEY CLUSTERED ([First Day of Week]);
GO

GRANT SELECT ON [main].[paj_jp_oil_stats_weekly_data] TO [IEA_EXTERNAL-DB_READ]
GO
