I want you to make a file that does the following.

1) There should be a single file called main.rs that contains all the code
2) The code will be passed a SINGLE SQL file
3) The code will parse the SQL file and search for CREATE TABLE statements
4) Write JUST the CREATE TABLE statements to a file called tables.sql
5) For each CREATE TABLE statement, the code will extract the table name and column names
6) For each table, the code will create a CSV file with the column names as the first row
6) The code will then parse the SQL file and search for INSERT statements
7) For each INSERT statement, the code will extract the table name and values
8) The code will then append the values to the CSV file for the corresponding table
9) The code will then close the CSV file


Keep the following points in mind while writing the code:
- Code must be modular and readable
- Code must use functional programming concepts and techniques
- Code must organize operations as a flow from input to output
- Code MUST use sqlparser with MySql dialect
- CREATE TABLE statements will be on multiple lines
- INSERT statements will be on a single line
- If insert statement fails to be parsed for any reason, ignore it and warn log it
- If create table statement fails to be parsed for any reason, error log it and exit
- All created files should be in the current working directory
- USE a lot of logging for transparency
- Assume insert statements contain MULTIPLE rows
- At start do a sanity check and remove any output files from previous runs
- Use AS MUCH parallelism as possible to speed up processing
- DO NOT leave unused imports
- DO NOT USE "?" operator. Use unwrap() always to verify results
- Use Ok type to handle errors where appropriate