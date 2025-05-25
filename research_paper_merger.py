import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional

def normalize_text(text):
    """Normalize text for comparison (case-insensitive, strip whitespace)"""
    if pd.isna(text) or text == '':
        return ''
    return str(text).strip().lower()

def is_valid_doi(doi):
    """Check if DOI is valid (not NaN, not empty string)"""
    return pd.notna(doi) and str(doi).strip() != ''

def get_column_names(file_config: Dict, default_title: str, default_abstract: str, default_doi: str) -> Tuple[str, str, str]:
    """Get column names for a specific file configuration"""
    title_col = file_config.get('title_col', default_title)
    abstract_col = file_config.get('abstract_col', default_abstract)
    doi_col = file_config.get('doi_col', default_doi)
    return title_col, abstract_col, doi_col

def process_csv_file(file_config: Dict, default_title: str, default_abstract: str, default_doi: str) -> pd.DataFrame:
    """Load and process a single CSV file"""
    filepath = file_config['file']
    db_name = file_config['label']
    title_col, abstract_col, doi_col = get_column_names(file_config, default_title, default_abstract, default_doi)
    
    try:
        df = pd.read_csv(filepath)
        print(f"Loaded {filepath}: {len(df)} rows")
        
        # Check if required columns exist
        required_cols = [title_col, abstract_col, doi_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing columns in {filepath}: {missing_cols}")
            # Add missing columns with NaN values
            for col in missing_cols:
                df[col] = np.nan
        
        # Standardize column names and keep only required columns
        df_processed = pd.DataFrame()
        df_processed['Title'] = df[title_col]
        df_processed['Abstract'] = df[abstract_col]
        df_processed['DOI'] = df[doi_col]
        df_processed['DB'] = db_name
        
        # Normalize DOI and Title for comparison
        df_processed['doi_normalized'] = df_processed['DOI'].apply(normalize_text)
        df_processed['title_normalized'] = df_processed['Title'].apply(normalize_text)
        
        return df_processed
        
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return pd.DataFrame(columns=['Title', 'Abstract', 'DOI', 'DB', 'doi_normalized', 'title_normalized'])
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return pd.DataFrame(columns=['Title', 'Abstract', 'DOI', 'DB', 'doi_normalized', 'title_normalized'])

def log_row_decision(log_df: pd.DataFrame, row: pd.Series, idx: int, db_name: str, selected: bool, reason: str) -> pd.DataFrame:
    """Log the decision for a single row"""
    log_entry = {
        'Source_File': db_name,
        'Row_Index': idx + 1,  # 1-based indexing for readability
        'Title': row['Title'] if pd.notna(row['Title']) else '',
        'Abstract': row['Abstract'] if pd.notna(row['Abstract']) else '',
        'DOI': row['DOI'] if pd.notna(row['DOI']) else '',
        'Selected': 'YES' if selected else 'NO',
        'Reason': reason
    }
    return pd.concat([log_df, pd.DataFrame([log_entry])], ignore_index=True)

def print_summary(files_config: List[Dict], stats: Dict, merged_df: pd.DataFrame, log_df: pd.DataFrame, output_file: str, log_file: str):
    """Print detailed summary statistics"""
    print("\n=== SUMMARY STATISTICS ===")
    print(f"{'Database':<10} {'Total Rows':<12} {'DOI-based':<12} {'Title-only':<12} {'Total Added':<12}")
    print("-" * 60)
    
    total_original = 0
    total_added = 0
    
    for file_config in files_config:
        db_name = file_config['label']
        total_rows = stats['total_rows'][db_name]
        doi_added = stats['unique_doi_added'][db_name]
        title_added = stats['unique_title_added'][db_name]
        total_db_added = stats['total_unique_added'][db_name]
        
        print(f"{db_name:<10} {total_rows:<12} {doi_added:<12} {title_added:<12} {total_db_added:<12}")
        
        total_original += total_rows
        total_added += total_db_added
    
    print("-" * 60)
    print(f"{'TOTAL':<10} {total_original:<12} {'':<12} {'':<12} {total_added:<12}")
    
    print(f"\nDeduplication Results:")
    print(f"  - Original total rows: {total_original}")
    print(f"  - Final unique papers: {total_added}")
    print(f"  - Duplicates removed: {total_original - total_added}")
    if total_original > 0:
        print(f"  - Deduplication rate: {((total_original - total_added) / total_original * 100):.1f}%")
    
    print(f"\nFiles created:")
    print(f"  - {output_file}: Final merged dataset ({len(merged_df)} unique papers)")
    print(f"  - {log_file}: Detailed log of every row processed ({len(log_df)} total rows)")
    
    # Log file summary
    if len(log_df) > 0:
        selected_count = len(log_df[log_df['Selected'] == 'YES'])
        rejected_count = len(log_df[log_df['Selected'] == 'NO'])
        print(f"\nLog Summary:")
        print(f"  - Total rows processed: {len(log_df)}")
        print(f"  - Rows selected: {selected_count}")
        print(f"  - Rows rejected: {rejected_count}")
        print(f"  - Selection rate: {(selected_count / len(log_df) * 100):.1f}%")

def merge_research_papers(files_config: Optional[List[Dict]] = None,
                         title_col: str = 'Title',
                         abstract_col: str = 'Abstract', 
                         doi_col: str = 'DOI',
                         output_file: str = 'all.csv',
                         log_file: str = 'processing_log.csv'):
    """
    Merge research paper CSV files with intelligent deduplication.
    
    Args:
        files_config: List of file configurations
            Format: [{'file': 'path.csv', 'label': 'DB_Name', 'title_col': 'custom_title', ...}, ...]
            Default: Uses scopus.csv, ieee.csv, pubmed.csv, wos.csv
        title_col: Default column name for titles
        abstract_col: Default column name for abstracts
        doi_col: Default column name for DOIs
        output_file: Output merged file name
        log_file: Log file name
    
    Returns:
        tuple: (merged_dataframe, log_dataframe, statistics_dict)
    
    Example usage:
        # Default usage
        merge_research_papers()
        
        # Custom files
        files = [
            {'file': 'data1.csv', 'label': 'Database1'},
            {'file': 'data2.csv', 'label': 'Database2', 'title_col': 'Paper_Title'}
        ]
        merged_df, log_df, stats = merge_research_papers(files_config=files)
    """
    
    # Default configuration
    if files_config is None:
        files_config = [
            {'file': 'scopus.csv', 'label': 'Scopus'},
            {'file': 'ieee.csv', 'label': 'IEEE'},
            {'file': 'pubmed.csv', 'label': 'PubMed'},
            {'file': 'wos.csv', 'label': 'WOS'}
        ]
    
    # Initialize tracking
    seen_dois = set()
    seen_titles = set()
    merged_df = pd.DataFrame(columns=['Title', 'Abstract', 'DOI', 'DB'])
    log_df = pd.DataFrame(columns=['Source_File', 'Row_Index', 'Title', 'Abstract', 'DOI', 'Selected', 'Reason'])
    
    # Statistics tracking
    stats = {
        'total_rows': {},
        'unique_doi_added': {},
        'unique_title_added': {},
        'total_unique_added': {}
    }
    
    print("=== Research Paper CSV Merger ===\n")
    
    # Process each file in order
    for file_config in files_config:
        db_name = file_config['label']
        print(f"Processing {db_name} ({file_config['file']})...")
        
        # Load and process the file
        df = process_csv_file(file_config, title_col, abstract_col, doi_col)
        
        if df.empty:
            stats['total_rows'][db_name] = 0
            stats['unique_doi_added'][db_name] = 0
            stats['unique_title_added'][db_name] = 0
            stats['total_unique_added'][db_name] = 0
            continue
        
        stats['total_rows'][db_name] = len(df)
        
        # Separate rows with valid DOIs and missing DOIs
        df_with_doi = df[df['doi_normalized'] != ''].copy()
        df_without_doi = df[df['doi_normalized'] == ''].copy()
        
        # Initialize counters for this database
        doi_added = 0
        title_added = 0
        intra_file_duplicates = 0
        
        # Process rows with valid DOIs first
        for idx, row in df_with_doi.iterrows():
            doi_norm = row['doi_normalized']
            
            if doi_norm not in seen_dois:
                seen_dois.add(doi_norm)
                # Also add title to seen_titles to prevent title-based duplicates
                if row['title_normalized']:
                    seen_titles.add(row['title_normalized'])
                
                # Add to merged dataframe (without normalized columns)
                row_to_add = row[['Title', 'Abstract', 'DOI', 'DB']].copy()
                merged_df = pd.concat([merged_df, row_to_add.to_frame().T], ignore_index=True)
                doi_added += 1
                
                log_df = log_row_decision(log_df, row, idx, db_name, True, 'Unique DOI')
            else:
                intra_file_duplicates += 1
                log_df = log_row_decision(log_df, row, idx, db_name, False, f'Duplicate DOI (already seen: {doi_norm})')
        
        # Process rows with missing DOIs (title-based deduplication)
        for idx, row in df_without_doi.iterrows():
            title_norm = row['title_normalized']
            
            if title_norm and title_norm not in seen_titles:
                seen_titles.add(title_norm)
                
                # Add to merged dataframe (without normalized columns)
                row_to_add = row[['Title', 'Abstract', 'DOI', 'DB']].copy()
                merged_df = pd.concat([merged_df, row_to_add.to_frame().T], ignore_index=True)
                title_added += 1
                
                log_df = log_row_decision(log_df, row, idx, db_name, True, 'Unique Title (no DOI)')
            elif title_norm:  # Only count as duplicate if title exists
                intra_file_duplicates += 1
                log_df = log_row_decision(log_df, row, idx, db_name, False, f'Duplicate Title (already seen: {title_norm[:50]}...)')
            elif not title_norm:  # Empty or missing title
                log_df = log_row_decision(log_df, row, idx, db_name, False, 'Empty/missing Title and DOI')
        
        # Update statistics
        stats['unique_doi_added'][db_name] = doi_added
        stats['unique_title_added'][db_name] = title_added
        stats['total_unique_added'][db_name] = doi_added + title_added
        
        print(f"  - Total rows: {stats['total_rows'][db_name]}")
        print(f"  - Unique rows added (DOI-based): {doi_added}")
        print(f"  - Unique rows added (Title-only): {title_added}")
        print(f"  - Total unique added: {doi_added + title_added}")
        if intra_file_duplicates > 0:
            print(f"  - Intra-file duplicates found: {intra_file_duplicates}")
        print()
    
    # Save the merged result
    merged_df.to_csv(output_file, index=False)
    print(f"Merged data saved to: {output_file}")
    
    # Save the detailed log
    log_df.to_csv(log_file, index=False)
    print(f"Detailed processing log saved to: {log_file}")
    
    print(f"Final dataset contains: {len(merged_df)} unique papers")
    
    # Print summary statistics
    print_summary(files_config, stats, merged_df, log_df, output_file, log_file)
    
    return merged_df, log_df, stats


if __name__ == "__main__":
    # Default usage - processes scopus.csv, ieee.csv, pubmed.csv, wos.csv
    merge_research_papers()