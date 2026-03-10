CREATE TABLE [dbo_marts].[dim_fiscal_periods] (

	[fiscal_period_key] varchar(400) NULL, 
	[fiscal_period_id] varchar(10) NULL, 
	[fiscal_year] int NULL, 
	[fiscal_month] int NULL, 
	[fiscal_quarter] varchar(16) NULL, 
	[period_start_date] date NULL, 
	[period_end_date] date NULL, 
	[is_closed] int NOT NULL, 
	[period_display_name] varchar(29) NOT NULL
);