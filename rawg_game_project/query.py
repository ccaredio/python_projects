import pandas as pd
import duckdb

# Connect to your persistent DuckDB file
con = duckdb.connect('rawg_data.duckdb')

# Run a simple query to get the first 10 rows from your games_raw table
query = "SELECT * FROM games_raw"

# Execute the query and fetch results into a pandas DataFrame
df = con.execute(query).fetchdf()

schema_df = con.execute("PRAGMA table_info('games_raw')").fetchdf()

# Show the preview of your data
print(df.head())

print(schema_df)

# Close the connection when done
con.close()