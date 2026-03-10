CREATE TABLE [dbo_marts].[fact_gl_activity] (

	[account_key] varchar(400) NULL, 
	[cost_center_key] varchar(400) NULL, 
	[fiscal_period_key] varchar(400) NULL, 
	[journal_entry_id] varchar(20) NULL, 
	[line_number] int NULL, 
	[posting_date] date NULL, 
	[je_description] varchar(200) NULL, 
	[source_system] varchar(50) NULL, 
	[je_status] varchar(20) NULL, 
	[debit_amount] decimal(18,2) NULL, 
	[credit_amount] decimal(18,2) NULL, 
	[net_amount] decimal(19,2) NULL
);