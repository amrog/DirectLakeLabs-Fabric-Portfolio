[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "departments csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"departments csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "departments.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 6, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"department_id", type text}, {"department_name", type text}, {"division", type text}, {"location", type text}, {"cost_center", type text}, {"head_count_budget", Int64.Type}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "employees csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"employees csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "employees.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 17, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"employee_id", type text}, {"first_name", type text}, {"last_name", type text}, {"email", type text}, {"department_id", type text}, {"job_title", type text}, {"hire_date", type date}, {"termination_date", type date}, {"termination_reason", type text}, {"employment_type", type text}, {"salary", Int64.Type}, {"gender", type text}, {"ethnicity", type text}, {"education_level", type text}, {"manager_id", type text}, {"is_active", type logical}, {"location", type text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type", "salary_cleaned", each if [salary] < 0 then [salary] * 1 else [salary]),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted conditional column", {"employee_id", "first_name", "last_name", "email", "department_id", "job_title", "hire_date", "termination_date", "termination_reason", "employment_type", "salary", "salary_cleaned", "gender", "ethnicity", "education_level", "manager_id", "is_active", "location"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"salary"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"salary_cleaned", "salary"}}),
  #"Filtered rows" = Table.SelectRows(#"Renamed columns", each [hire_date] < #date(2026, 1, 1)),
  #"Added custom" = Table.AddColumn(#"Filtered rows", "tenure_years", each Number.Round(Duration.TotalDays(Date.From(DateTime.LocalNow()) - [hire_date]) / 365.25, 2)),
  #"Trimmed text" = Table.TransformColumns(#"Added custom", {{"first_name", each Text.Trim(_), type nullable text}, {"last_name", each Text.Trim(_), type nullable text}, {"email", each Text.Trim(_), type nullable text}, {"job_title", each Text.Trim(_), type nullable text}}),
  #"Cleaned text" = Table.TransformColumns(#"Trimmed text", {{"first_name", each Text.Clean(_), type nullable text}, {"last_name", each Text.Clean(_), type nullable text}, {"email", each Text.Clean(_), type nullable text}, {"job_title", each Text.Clean(_), type nullable text}}),
  #"Inserted merged column" = Table.AddColumn(#"Cleaned text", "full_name", each Text.Combine({[last_name], [first_name]}, " "), type text),
  #"Reordered columns 1" = Table.ReorderColumns(#"Inserted merged column", {"employee_id", "first_name", "last_name", "full_name", "email", "department_id", "job_title", "hire_date", "termination_date", "termination_reason", "employment_type", "salary", "gender", "ethnicity", "education_level", "manager_id", "is_active", "location", "tenure_years"}),
  #"Filtered rows 1" = Table.SelectRows(#"Reordered columns 1", each ([salary] <> -78000 and [salary] <> -71000)),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows 1", {{"tenure_years", Int64.Type}, {"salary", Int64.Type}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "performance_reviews csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"performance_reviews csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "performance_reviews.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"review_id", type text}, {"employee_id", type text}, {"review_cycle", type text}, {"overall_rating", Int64.Type}, {"goals_met_pct", Int64.Type}, {"reviewer_id", type text}, {"review_date", type date}, {"comments", type text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type", "performance_band", each if [overall_rating] <= 5 then "Exceptional" else if [overall_rating] <= 4 then "Exceeds Expectations" else if [overall_rating] <= 3 then "Meets Expectations" else if [overall_rating] >= 2 then "Needs Improvement" else "Unsatisfactory"),
  #"Removed columns" = Table.RemoveColumns(#"Inserted conditional column", {"comments"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"performance_band", type text}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "salary_history csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"salary_history csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "salary_history.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 6, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"salary_history_id", type text}, {"employee_id", type text}, {"effective_date", type date}, {"salary_amount", Int64.Type}, {"change_type", type text}, {"change_pct", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type", "salary_cleaned ", each if [salary_amount] < 0 then [salary_amount] * 1 else [salary_amount]),
  #"Reordered columns" = Table.ReorderColumns(#"Inserted conditional column", {"salary_history_id", "employee_id", "effective_date", "salary_amount", "salary_cleaned ", "change_type", "change_pct"}),
  #"Filtered rows" = Table.SelectRows(#"Reordered columns", each ([salary_amount] <> -78000 and [salary_amount] <> -75000 and [salary_amount] <> -72000 and [salary_amount] <> -71000 and [salary_amount] <> -69000 and [salary_amount] <> -68000 and [salary_amount] <> -67000 and [salary_amount] <> -64000)),
  #"Removed columns" = Table.RemoveColumns(#"Filtered rows", {"salary_amount"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"salary_cleaned ", "salary_amount"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"salary_amount", Int64.Type}})
in
  #"Changed column type 1";
shared #"departments csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_departments", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"employees csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_employees", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"salary_history csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_salary_history", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"performance_reviews csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "43d31b4f-3433-413a-b1c7-24d6452587f4"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "aefedc79-f516-4dd5-802f-6908eab8505b"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_performance_reviews", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
