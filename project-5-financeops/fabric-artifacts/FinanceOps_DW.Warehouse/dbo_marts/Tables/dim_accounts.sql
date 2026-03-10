CREATE TABLE [dbo_marts].[dim_accounts] (

	[account_key] varchar(400) NULL, 
	[account_id] varchar(10) NULL, 
	[account_name] varchar(28) NULL, 
	[account_type] varchar(16) NULL, 
	[financial_statement] varchar(16) NULL, 
	[account_subgroup] varchar(22) NULL, 
	[is_active] int NOT NULL, 
	[normal_balance_type] varchar(6) NULL
);