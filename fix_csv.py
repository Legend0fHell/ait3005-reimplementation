import pandas as pd
import re

# Read the CSV files
df1 = pd.read_csv('summarized_articles_0321.csv')
df2 = pd.read_csv('summarized_articles_checkpoint_8.csv')

# Merge the DataFrames
merged_df = pd.concat([df1, df2], ignore_index=True)

# Sort by date
merged_df['parsed_date'] = pd.to_datetime(merged_df['date'])
merged_df = merged_df.sort_values('parsed_date')

# Clean the group column
def clean_group(group):
    # Remove numbers, dots, and star symbols
    clean = re.sub(r'[0-9.*]', '', str(group))
    # Remove () or []
    clean = re.sub(r'[\(\)\[\]]', '', clean)
    # Trim whitespace
    clean = clean.strip()
    return clean

merged_df['group'] = merged_df['group'].apply(clean_group)

# Re-index the postID column
merged_df['postID'] = range(1, len(merged_df) + 1)

# Drop the parsed_date column used for sorting
merged_df = merged_df.drop('parsed_date', axis=1)

# Save the result
merged_df.to_csv('summarized_articles_merged.csv', index=False)

print(f"Merged {len(df1)} and {len(df2)} records into {len(merged_df)} records")
print(f"Saved to summarized_articles_merged.csv")
