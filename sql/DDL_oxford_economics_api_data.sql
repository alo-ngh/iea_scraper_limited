drop table [main].[oxford_economics_api_data];
go

CREATE TABLE [main].[oxford_economics_api_data](
	[databankcode] [varchar](7) NOT NULL,
	[producttypecode] [varchar](3) NOT NULL,
	[locationcode] [varchar](3) NOT NULL,
	[variablecode] [varchar](6) NOT NULL,
	[measurecode] [varchar](1) NOT NULL,
	[quarter] [varchar](1) NULL,
	[metadata_description] [varchar](200) NULL,
	[metadata_location] [varchar](100) NULL,
	[metadata_databankname] [varchar](50) NULL,
	[metadata_scalefactor] [varchar](30) NULL,
	[metadata_authoremail] [varchar](50) NULL,
	[metadata_author] [varchar](30) NULL,
	[metadata_authortelephone] [varchar](20) NULL,
	[metadata_historicalendyear] [int] NULL,
	[metadata_historicalendquarter] [int] NULL,
	[metadata_imposedendyear] [int] NULL,
	[metadata_imposedendquarter] [int] NULL,
	[metadata_baseyearprice] [int] NULL,
	[metadata_lastupdate] [date] NULL,
	[metadata_seasonallyadjusted] [bit] NULL,
	[metadata_sectorcoverage] [varchar](30) NULL,
	[metadata_baseyearindex] [varchar](30) NULL,
	[metadata_sourcedetails] [varchar](300) NULL,
	[metadata_units] [varchar](30) NULL,
	[metadata_source] [varchar](300) NULL,
	[metadata_additionalsourcedetails] [varchar](300) NULL,
	[metadata_measurename] [varchar](10) NULL,
	[metadata_annualtypecode] [varchar](1) NULL,
	[metadata_partnername] [varchar](20) NULL,
	[metadata_indicatorname] [varchar](200) NULL,
	[metadata_scenarioname] [varchar](30) NULL,
	[metadata_commodityname] [varchar](30) NULL,
	[metadata_marketsectorname] [varchar](30) NULL,
	[metadata_incomebandname] [varchar](30) NULL,
	[metadata_hasquarterly] [bit] NULL,
	[metadata_categorydescription] [varchar](50) NULL,
	[value] [float] NULL,
	[frequency] [varchar](13) NOT NULL,
	[period] [int] NOT NULL,
	[date_created] [datetime] NULL,
	[date_modified] [datetime] NULL
)
GO

CREATE CLUSTERED COLUMNSTORE INDEX cci_oxford_economics_api_data on [main].[oxford_economics_api_data];
go

GRANT SELECT ON [main].[oxford_economics_api_data] TO [IEA_EXTERNAL-DB_READ]
GO