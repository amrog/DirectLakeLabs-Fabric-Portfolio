CREATE TABLE [dbo_marts].[dim_cost_centers] (

	[cost_center_key] varchar(400) NULL, 
	[cost_center_id] varchar(10) NULL, 
	[cost_center_name] varchar(24) NULL, 
	[department_group] varchar(16) NULL, 
	[manager_name] varchar(16) NULL, 
	[is_active] int NOT NULL
);