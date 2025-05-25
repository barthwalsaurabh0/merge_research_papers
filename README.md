# Research Paper CSV Merger

A Python library for merging research paper CSV files from multiple databases with deduplication and detailed logging usefull for things like SLR(Systematic Literature Review).

## Features

- **Smart Deduplication**: Uses DOI when available, falls back to title-based deduplication
- **Multiple Database Support**: Merge files from Scopus, IEEE, PubMed, Web of Science, or any custom databases
- **Flexible Column Mapping**: Handle different column names across databases
- **Detailed Logging**: Track every row's processing decision with reasons
- **Comprehensive Statistics**: Get detailed reports on merging and deduplication results
- **Configurable**: Customize file paths, column names, and processing order



## Quick Start

### Default Usage
```python
from paper_merger import merge_research_papers

# Processes scopus.csv, ieee.csv, pubmed.csv, wos.csv
# Expects columns: Title, Abstract, DOI
merge_research_papers()
```

This creates:
- `all.csv`: Merged dataset with unique papers
- `processing_log.csv`: Detailed log of all processing decisions

## Usage Examples

### 1. Custom Files with Default Column Names
```python
files = [
    {'file': 'database1.csv', 'label': 'DB1'},
    {'file': 'database2.csv', 'label': 'DB2'},
    {'file': 'database3.csv', 'label': 'DB3'}
]

merge_research_papers(files_config=files)
```

### 2. Different Column Names
```python
# Global column name changes
merge_research_papers(
    title_col='Paper_Title',
    abstract_col='Summary', 
    doi_col='Digital_Object_Identifier'
)
```

### 3. Per-File Custom Column Names
```python
files = [
    {
        'file': 'scopus_export.csv', 
        'label': 'Scopus',
        'title_col': 'Article_Title',
        'abstract_col': 'Abstract_Text',
        'doi_col': 'DOI_Number'
    },
    {
        'file': 'ieee_data.csv', 
        'label': 'IEEE',
        'title_col': 'Title',
        'abstract_col': 'Summary'
        # Uses default DOI column name
    },
    {
        'file': 'pubmed.csv',
        'label': 'PubMed'
        # Uses all default column names
    }
]

merge_research_papers(files_config=files)
```

### 4. Custom Output Files
```python
merge_research_papers(
    output_file='merged_papers.csv',
    log_file='detailed_processing_log.csv'
)
```

### 5. Return Data for Further Processing
```python
merged_df, log_df, stats = merge_research_papers()

# Work with the dataframes directly
print(f"Total unique papers: {len(merged_df)}")
print(f"Scopus papers selected: {stats['total_unique_added']['Scopus']}")

# Filter by database
scopus_papers = merged_df[merged_df['DB'] == 'Scopus']
```

## File Configuration Format

Each file configuration is a dictionary with the following keys:

```python
{
    'file': 'path/to/file.csv',           # Required: Path to CSV file
    'label': 'Database_Name',             # Required: Label for the database
    'title_col': 'Custom_Title_Column',   # Optional: Custom title column name
    'abstract_col': 'Custom_Abstract',    # Optional: Custom abstract column name
    'doi_col': 'Custom_DOI_Column'        # Optional: Custom DOI column name
}
```

## Deduplication Logic

The library follows this deduplication strategy:

1. **Processing Order**: Files are processed in the order specified
2. **DOI-based Deduplication**: 
   - Papers with valid DOIs are deduplicated first
   - Case-insensitive comparison with whitespace trimming
3. **Title-based Deduplication**:
   - For papers without DOIs, uses title-based deduplication
   - Case-insensitive comparison
4. **Intra-file Deduplication**: Removes duplicates within the same file
5. **Cross-file Deduplication**: Prevents duplicates across different files

## Output Files

### Merged Dataset (`all.csv`)
Contains the final merged dataset with columns:
- `Title`: Paper title
- `Abstract`: Paper abstract  
- `DOI`: Digital Object Identifier
- `DB`: Source database label

### Processing Log (`processing_log.csv`)
Detailed log of every row processed with columns:
- `Source_File`: Origin database
- `Row_Index`: Original row number in source file
- `Title`: Paper title
- `Abstract`: Paper abstract
- `DOI`: DOI value
- `Selected`: YES/NO indicating if included in final dataset
- `Reason`: Explanation for the decision

#### Possible Reasons:
- `Unique DOI`: Selected due to unique DOI
- `Unique Title (no DOI)`: Selected due to unique title (no DOI available)
- `Duplicate DOI (already seen: ...)`: Rejected due to duplicate DOI
- `Duplicate Title (already seen: ...)`: Rejected due to duplicate title
- `Empty/missing Title and DOI`: Rejected due to missing both title and DOI

## Statistics Output

The library provides comprehensive statistics:

```
=== SUMMARY STATISTICS ===
Database   Total Rows   DOI-based    Title-only   Total Added 
----------------------------------------------------------
Scopus     338          312          24           336        
IEEE       89           85           3            88         
PubMed     156          145          8            153        
WOS        234          198          15           213        
----------------------------------------------------------
TOTAL      817                                    790        

Deduplication Results:
  - Original total rows: 817
  - Final unique papers: 790
  - Duplicates removed: 27
  - Deduplication rate: 3.3%
```

## Error Handling

The library gracefully handles common issues:
- **Missing files**: Continues processing other files
- **Missing columns**: Adds empty columns and warns user
- **Empty files**: Skips and continues processing
- **Malformed data**: Handles NaN values and empty strings

## Samples

**sample/** contain sample DBs (scopus.csv, ieee.csv, pubmed.csv, wos.csv) and final all.csv and processing_log.csv.

## Requirements

- Python 3.6+
- pandas
- numpy

## License

MIT License - feel free to use in your research projects.
