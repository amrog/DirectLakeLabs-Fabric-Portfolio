CREATE TABLE [dbo_marts].[rpt_budget_vs_actuals] (

	[fiscal_period_id] varchar(10) NULL, 
	[account_id] varchar(10) NULL, 
	[cost_center_id] varchar(10) NULL, 
	[budget_amount] decimal(18,2) NULL, 
	[actual_amount] decimal(38,2) NULL, 
	[account_name] varchar(28) NULL, 
	[account_type] varchar(16) NULL, 
	[account_subgroup] varchar(22) NULL, 
	[cost_center_name] varchar(24) NULL, 
	[department_group] varchar(16) NULL, 
	[fiscal_year] int NULL, 
	[fiscal_month] int NULL, 
	[fiscal_quarter] varchar(16) NULL, 
	[variance_amount] decimal(38,2) NULL, 
	[variance_pct] decimal(38,20) NULL, 
	[variance_status] varchar(11) NOT NULL
);