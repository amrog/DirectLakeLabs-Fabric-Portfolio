CREATE TABLE [dbo_marts].[rpt_income_statement] (

	[fiscal_period_id] varchar(10) NULL, 
	[fiscal_year] int NULL, 
	[fiscal_month] int NULL, 
	[fiscal_quarter] varchar(16) NULL, 
	[operating_revenue] decimal(38,2) NULL, 
	[other_revenue] decimal(38,2) NULL, 
	[cost_of_revenue] decimal(38,2) NULL, 
	[operating_expenses] decimal(38,2) NULL, 
	[non_operating_expenses] decimal(38,2) NULL, 
	[total_revenue] decimal(38,2) NULL, 
	[gross_profit] decimal(38,2) NULL, 
	[operating_income] decimal(38,2) NULL, 
	[net_income] decimal(38,2) NULL, 
	[gross_margin_pct] decimal(38,20) NULL, 
	[operating_margin_pct] decimal(38,20) NULL
);