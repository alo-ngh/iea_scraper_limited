SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE SCHEMA [argus_prices];
GO

CREATE TABLE [argus_prices].[argus_prices_data](
	[Code] [varchar](9) NULL,
	[TS Type] int NOT NULL,
	[PT Code] int NOT NULL,
	[Date] datetime NOT NULL,
	[Value] float NOT NULL,
	[Fwd Period] int NOT NULL,
	[Diff Base Roll] int NOT NULL,
	[Year] int NOT NULL,
	[Cont Fwd] int NOT NULL,
	[Record Status] [varchar](1) NOT NULL,
	[Module] [varchar](20) NOT NULL,
	[Source] [varchar](50) NOT NULL,
	[date_created] datetime NULL,
	[date_modified] datetime NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

GRANT SELECT ON [argus_prices].[argus_prices_data] TO [IEA_EXTERNAL-DB_READ]

GO

CREATE CLUSTERED COLUMNSTORE INDEX [cci_argus_prices_data]
ON [argus_prices].[argus_prices_data] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO


CREATE TABLE [argus_prices].[forward_curves_data](
	[Product] [bigint] NULL,
	[Market] [bigint] NULL,
	[ValuationType] [bigint] NULL,
	[NatGasLocationReference] [bigint] NULL,
	[OptionStrikePrice] [bigint] NULL,
	[Term] [bigint] NULL,
	[PromptIndicator] [bigint] NULL,
	[ForwardPeriod] [bigint] NULL,
	[Year] [bigint] NULL,
	[TradeDate] [datetime] NULL,
	[Unit] [bigint] NULL,
	[Value] [float] NULL,
	[FCRepositoryId] [bigint] NULL,
	[RecordStatus] [varchar](1) NULL,
	[Source] [varchar](50) NULL,
	[Folder] [varchar](50) NULL,
	[date_created] [datetime] NULL,
	[date_modified] [datetime] NULL,
	[FileName] [varchar](20) NULL
) ON [PRIMARY]
GO

GO

GRANT SELECT ON [argus_prices].[forward_curves_data] TO [IEA_EXTERNAL-DB_READ]

GO

CREATE CLUSTERED COLUMNSTORE INDEX [cci_forward_curves_data]
ON [argus_prices].[forward_curves_data] WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [PRIMARY]
GO

create view [argus_prices].[V_ARGUS_PRICES_DATA] as
select
	 doc.Module,
	 doc.ModuleDescription,
	 doc.Folder,
	 doc.FileName,
	 doc.Category,
     d.Code,
	 CAST(RIGHT(d.Code, LEN(d.Code) - 2) AS int) NumericCode,
	 doc.DisplayName,
	 doc.ContinuousForwardPeriod,
	 doc.TimeStampID,
	 doc.TimeStamp,
	 doc.PriceTypeID,
	 doc.PriceType,
	 doc.DeliveryMode,
	 doc.Timing,
	 doc.ForwardPeriodDescription,
	 doc.DifferentialBasis,
	 doc.DifferentialBasisTiming,
	 doc.StartDate,
	 doc.EndDate,
	 doc.OldCode,
	 doc.Unit,
	 doc.Frequency,
	 doc.StartDateInModule,
	 doc.EndDateInModule,
	 doc.DecimalPlaces,
	 doc.Specification,
	 m.Time as [Module Time],
	 m.LocalTime as [Module LocalTime],
	 m.LocalTimeZone as [Module LocalTimeZone],
	 d.[Date],
	 d.[Year],
	 d.[Record Status],
	 d.Source,
	 d.[Value],
	 d.date_created,
	 d.date_modified
from argus_prices.argus_prices_data d
left outer join argus_prices.latestDoc doc
	 on d.Code = doc.Code
	 and d.[Cont Fwd] = doc.ContinuousForwardPeriod
	 and d.[PT Code] = doc.PriceTypeID
	 and d.[TS Type] = doc.TimeStampID
	 and d.FileName = doc.FileName
left outer join argus_prices.latestModules m
	 on d.FileName = m.FileName
where ((d.Date between doc.StartDate and doc.EndDate)
or doc.EndDate is null and d.Date >= doc.StartDate)

GO

GRANT SELECT ON [argus_prices].[V_ARGUS_PRICES_DATA] TO [IEA_EXTERNAL-DB_READ]

GO

create view [argus_prices].[V_FORWARD_CURVES_DATA] as
select
fcm.Module,
fcm.[Path],
fcm.[Description],
fcm.[Folder],
fcm.[Time] as [ModuleTime],
fcm.[LocalTime] as [ModuleLocalTime],
fcm.[LocalTimeZone] as [ModuleLocalTimeZone],
d.Product [ProductId],
p.Product as [Product],
fcmd.[StartDateInModule],
fcmd.[EndDateInModule],
d.Market [MarketId],
m.Market as [Market],
d.ValuationType as [ValuationTypeId],
v.ValuationType as [ValuationType],
d.NatGasLocationReference as [NatGasLocationReferenceId],
natgas.Market as [NatGasLocationReference],
d.OptionStrikePrice as [OptionStrikePriceId],
o.OptionStrikePrice as [OptionStrikePrice],
d.Term as [TermId],
t.Term as [Term],
d.PromptIndicator,
d.ForwardPeriod,
d.[Year],
d.TradeDate,
d.Unit as [UnitId],
u.Unit as [Unit],
d.[Value] as [Value],
d.FCRepositoryId,
d.RecordStatus,
d.Source,
d.[FileName],
d.date_created,
d.date_modified
from argus_prices.forward_curves_data d
left outer join argus_prices.latestFCModules fcm
on d.[FileName] = fcm.[FileName]
left outer join argus_prices.latestFCModuleDetails as fcmd
on d.[Product] = fcmd.[Product_id] and fcm.[Module] = fcmd.[Module]
left outer join argus_prices.latestProduct p
on d.Product = p.id
left outer join argus_prices.latestMarket m
on d.Market = m.id
left outer join argus_prices.latestValuationType v
on d.ValuationType = v.id
left outer join argus_prices.latestMarket natgas
on d.NatGasLocationReference = natgas.id
left outer join argus_prices.latestOptionStrikePrice o
on d.OptionStrikePrice = o.id
left outer join argus_prices.latestTerm t
on d.Term = t.id
left outer join argus_prices.latestUnit u
on d.Unit = u.id;

GO

GRANT SELECT ON [argus_prices].[V_FORWARD_CURVES_DATA] TO [IEA_EXTERNAL-DB_READ]

GO

create view [argus_prices].[V_UNIT_CONVERSION_RATIOS] as
select conv.BaseUnitID,
       unit.DESCRIPTION as [BaseUnitDescription],
       conv.UnitID,
       baseUnit.DESCRIPTION as [UnitDescription],
       conv.CodeID,
       conv.ValidFrom,
       conv.ValidTo,
       conv.Ratio
from argus_prices.latestUnitCodeConv conv
left outer join argus_prices.latestUnits unit
on unit.UNIT_ID = conv.BaseUnitID
left outer join argus_prices.latestUnits baseUnit
on baseUnit.UNIT_ID = conv.UnitID;

GO


GRANT SELECT ON [argus_prices].[V_UNIT_CONVERSION_RATIOS] TO [IEA_EXTERNAL-DB_READ]