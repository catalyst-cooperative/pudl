# Functions

* Imperative verbs (.e.g connect) should precede the object being acted upon (e.g. connect_db), unless the function returns a simple value (e.g. datadir).

* No duplication of information (e.g. form names)

* lowercase, underscores separate words

* Helper functions (functions used within a single module only) should be preceded by an underscore

* When we are segmenting data by a value or list of values, the name should appears as "<data to be sorted>\_" + “by\_” + “<values to sort on>” (e.g. “eia_plants_by_operator”.

* When the object is a table, use the full table name (e.g. ingest_fuel_ferc1)

* When dataframe outputs are built from multiple tables, identify the type of information being pulled (e.g. "plants_") and the source of the tables (e.g. “eia”or “ferc1”). When outputs are built from a single table, simply use the table name (e.g. “boiler_fuel_eia923”).

Abbreviations: "db" for database, “info” for information; “assn” for association, “dir” for directory, “dfs” for dataframes, “int” for integer

### Database Connection and Helper Functions

Candidate naming convention: The lower level namespace uses an imperative verb to identify that action that the function performs while the upper level namespace identifies the database that the user is interacting with.

Abbreviations: "db" for database

### Ingest Functions

Candidate naming convention: Aside from static tables and glue tables, the name should conform to the rule "ingest_" + “<table name in pudl>”.

Abbreviations: "info" for information; “assn” for association

### Wrapper Ingest Functions

Candidate naming convention: "ingest_" + “<module name>”

### Data Extraction Functions

Candidate naming convention: The lower level namespace uses an imperative verb to identify that action that the function performs while the upper level namespace identifies the dataset where extraction is occurring.

Abbreviations: "dir" for directory, “dfs” for dataframes

### Data Cleaning Functions

Candidate naming convention: The lower level namespace uses an imperative verb to identify that action that the function performs while the upper level namespace is the result of "clean_" + “pudl” or the dataset to be cleaned (e.g. “ferc1”).

Abbreviations: "int" for integer

### MCOE Components Output Functions

Candidate naming convention: These outputs functions produce inputs for MCOE calculations. "pull_"+”<table name>”

### Outputs Functions

Candidate naming convention: When dataframe outputs are built from multiple tables, identify the type of information being pulled (e.g. "plants_") and the source of the tables (e.g. “eia”or “ferc1”). When outputs are built from a single table, simply use the table name (e.g. “boiler_fuel_eia923”).

Abbreviations: "utils" for utilities

### Analysis Functions

Candidate naming convention: When we are segmenting data by a value or list of values, the name should appears as "<data to be sorted>\_" + “by\_” + “<values to sort on>” (e.g. “eia_plants_by_operator”. Where possible, the <data to be sorted> should be the table name.

Abbreviations: "expns" for expenses, “corr” for correlation, “frc” for fuel receipts and costs, “gf” for generation fuel

# Database Tables

See: this [article](http://www.vertabelo.com/blog/technical-articles/naming-conventions-in-database-modeling) on database naming conventions.

Currently: Table names in snake_case

Candidate naming convention: The data source or label (e.g. "plant_id_pudl") should follow the thing it is describing

In column names:

* "total" should come at the beginning of the name (e.g. “total_expns_production”)

* "plant_id" should specify data source (e.g. “plant_id_eia”)

* The data source or label (e.g. "plant_id_pudl") should follow the thing it is describing

* Units (including percentage ("pct" and per unit (“per unit)) should be used wherever possible and come at the end of the name (e.g. “net_generation_mwh”)

* It is implied that costs are in dollars

* It is implied that fuel quantities are in physical units

Abbreviations: "util" for utility, “assn” for association, “info” for information

Column abbreviations: "us" for United States, “abbr” for abbreviation, “q” for
quarter, “ferc” for FERC form 1, “util” for utility, “qty” for quantity, “avg”
for average, “num” for number, “expns” for expenses, “pwr” for power, “pct” for
percentage, ppm for parts per million

* \_code indicates the field contains a readable abbreviation from a finite list of values.
* \_id indicates the field contains a usually numerical reference to another table, which will not be intelligible without looking up the value in that other table.
* \_name indicates a longer human readable name.
* \_date indicates the field contains a Date type.
* \_type indicates the field contains a short but complete human readable description (usually 1-3 words) from a list of finite valid values.
* capacity refers to nameplate capacity -- other more specific types of capacity are annotated.
* Regardless of what label utilities are given in the original data source (e.g. operator in EIA or respondent in FERC) we refer to them as utilities in all tables.

### Constants Tables

Candidate naming convention: constants table names are snake_case and plural because these tables represent existing, mostly static, collections of information (e.g. the list of U.S. states). The data source or label (e.g. "plant_id_pudl") should follow the thing it is describing.

Columns make use of the following abbreviations: "us" for United States, “abbr” for abbreviation, “q” for quarter, “ferc” for FERC form 1

### Glue Tables

Candidate naming convention: When constructed from a single data source "<object type>\_" + “source of information”. When constructed from multiple data sources “<object type>”.

Abbreviations: "util" for utility, “assn” for association

Columns make use of the following abbreviations:

* "Util" for utility

### Tables About Entities

Candidate naming convention: Where a FERC Form 1 or EIA tab/page name exists, "<page name>\_" + “<data source>” (e.g. “fuel_ferc1”). Without a page name, “<general subject>\_” + “<data source>” (e.g. “coalmine_eia923”).

In column names:

* "total" should come at the beginning of the name (e.g. “total_expns_production”)

* "plant_id" should specify data source (e.g. “plant_id_eia”)

* The data source or label (e.g. "plant_id_pudl") should follow the thing it is describing

* Units (including percentage ("pct" and per unit (“per unit)) should be used wherever possible and come at the end of the name (e.g. “net_generation_mwh”)

* It is implied that costs are in dollars

* It is implied that fuel quantities are in physical units

Abbreviations: "assn" for association

Columns make use of the following abbreviations:

* "Qty" for quantity, “avg” for average, “num” for number, “expns” for expenses, “pwr” for power, “abbr” for abbreviation”, “pct” for percentage
