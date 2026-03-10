CREATE TABLE [dbo_marts].[rpt_trial_balance] (

	[fiscal_period_id] varchar(10) NULL, 
	[fiscal_year] int NULL, 
	[fiscal_month] int NULL, 
	[fiscal_quarter] varchar(16) NULL, 
	[account_id] varchar(10) NULL, 
	[account_name] varchar(28) NULL, 
	[account_type] varchar(16) NULL, 
	[financial_statement] varchar(16) NULL, 
	[account_subgroup] varchar(22) NULL, 
	[total_debits] decimal(38,2) NULL, 
	[total_credits] decimal(38,2) NULL, 
	[net_activity] decimal(38,2) NULL, 
	[ending_balance] decimal(38,2) NULL, 
	[reported_balance] decimal(38,2) NULL
);