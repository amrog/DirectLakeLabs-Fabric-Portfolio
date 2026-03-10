CREATE TABLE [raw].[journal_entry_lines] (

	[journal_entry_id] varchar(20) NULL, 
	[line_number] varchar(20) NULL, 
	[account_id] varchar(10) NULL, 
	[cost_center_id] varchar(10) NULL, 
	[debit_amount] varchar(50) NULL, 
	[credit_amount] varchar(50) NULL, 
	[line_description] varchar(200) NULL
);