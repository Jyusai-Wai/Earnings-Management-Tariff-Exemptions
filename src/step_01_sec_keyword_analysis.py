import pandas as pd
import ast
from collections import defaultdict
from tqdm import tqdm
import re
import html
import warnings
import os
from sec_api import QueryApi, ExtractorApi
import config

# Ignore some warnings that do not affect the outcome
warnings.filterwarnings("ignore")

class SECKeywordCounter:
    """
    A class to fetch SEC filings, parse specified items, and count predefined keywords.
    """
    def __init__(self, api_key, keyword_path, start_year, end_year, form_type, items):
        self.sec_api_key = api_key
        self.keyword_csv_path = keyword_path
        self.start_year = start_year
        self.end_year = end_year
        self.form_type = form_type
        self.items = items
        self.keywords = self._load_keywords()
        self.query_api = QueryApi(api_key=self.sec_api_key)
        self.extractor_api = ExtractorApi(api_key=self.sec_api_key)
        if self.keywords:
            print(f"Successfully loaded {len(self.keywords)} keywords for initial scan.")

    def _load_keywords(self):
        try:
            df = pd.read_csv(self.keyword_csv_path)
            return list(df["keyword"])
        except FileNotFoundError:
            print(f"ERROR: Keyword file not found at '{self.keyword_csv_path}'.")
            return []

    @staticmethod
    def clean_text(text: str) -> str:
        text = html.unescape(text)
        text = text.replace("\n", " ")
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _count_keywords_in_text(self, text: str) -> dict:
        if not text:
            return {}
        text_lc = text.lower()
        return {kw: text_lc.count(kw.lower()) for kw in self.keywords if kw.lower() in text_lc}

    def fetch_filings(self):
        filing_index = []
        print("Fetching filing index from EDGAR...")
        for year in range(self.start_year, self.end_year + 1):
            print(f"Querying for year {year}...")
            query = (
                f'formType:"{self.form_type}" AND NOT formType:"{self.form_type}/A" '
                f'AND filedAt:[{year}-01-01 TO {year}-12-31]'
            )
            for from_index in range(0, 10000, 200): # Larger page size
                query_payload = {
                    "query": {"query_string": {"query": query}},
                    "from": from_index,
                    "size": 200,
                    "sort": [{"filedAt": {"order": "asc"}}]
                }
                try:
                    response = self.query_api.get_filings(query_payload)
                    filings = response.get("filings", [])
                    if not filings:
                        break
                    for filing in filings:
                        filing_index.append({
                            "cik": filing.get("cik", ""),
                            "file_date": filing.get("filedAt", "").split("T")[0],
                            "report_date": filing.get("periodOfReport", ""),
                            "company_name": filing.get("companyName", ""),
                            "filing_url": filing.get("linkToFilingDetails", ""),
                            "sic": filing.get("sic", "")
                        })
                except Exception as e:
                    print(f"Error fetching filings for year {year}: {e}")
                    break
        return pd.DataFrame(filing_index)

    def _parse_single_filing(self, filing_url):
        items_content = {}
        for item in self.items:
            try:
                raw_text = self.extractor_api.get_section(filing_url, item, "text")
                items_content[f'item_{item.lower()}_text'] = self.clean_text(raw_text)
            except Exception:
                items_content[f'item_{item.lower()}_text'] = ""
        return items_content

    def process_filings(self):
        filings_df = self.fetch_filings()
        if filings_df.empty:
            print("No filings found. Exiting.")
            return pd.DataFrame()

        all_results = []
        for _, row in tqdm(filings_df.iterrows(), total=len(filings_df), desc="Processing Filings"):
            current_result = row.to_dict()
            parsed_data = self._parse_single_filing(row['filing_url'])
            
            for item in self.items:
                item_text = parsed_data.get(f'item_{item.lower()}_text', "")
                keyword_counts = self._count_keywords_in_text(item_text)
                current_result[f'item_{item.lower()}_keyword_counts'] = str(keyword_counts) if keyword_counts else "{}"

            all_results.append(current_result)
        
        return pd.DataFrame(all_results)

def filter_relevant_firms(df):
    print("\nFiltering for firms discussing tariffs, policy, AND exemptions...")
    final_filings = []
    
    item_cols = [f'item_{i.lower()}_keyword_counts' for i in config.ITEMS_TO_ANALYZE]

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Filtering Relevant Firms"):
        combined_text = ""
        # This part is simplified; we just need to check if keywords exist.
        # A more robust check would involve parsing the count dictionaries.
        # For simplicity, we check the string representation.
        
        # Aggregate all keyword count strings
        all_counts_str = ""
        for col in item_cols:
            if pd.notna(row[col]):
                all_counts_str += row[col].lower()
        
        has_tariff = any(kw in all_counts_str for kw in config.TARIFF_KEYWORDS)
        has_policy = any(kw in all_counts_str for kw in config.POLICY_KEYWORDS)
        has_exemption = any(kw in all_counts_str for kw in config.EXEMPTION_KEYWORDS)

        if has_tariff and has_policy and has_exemption:
            final_filings.append(row)
            
    if not final_filings:
        print("Warning: No firms matched the combined keyword criteria.")
        return pd.DataFrame()

    return pd.DataFrame(final_filings)


def run_keyword_analysis():
    """
    Main function for Step 1. Fetches SEC filings, counts keywords,
    filters for relevant firms, and saves the output.
    """
    print("--- Starting Step 1: SEC Keyword Analysis ---")
    analyzer = SECKeywordCounter(
        api_key=config.SEC_API_KEY,
        keyword_path=config.KEYWORD_FILE_PATH,
        start_year=config.GLOBAL_PREDICTION_START_YEAR, # Focus on prediction period
        end_year=config.GLOBAL_PREDICTION_END_YEAR,
        form_type=config.FORM_TYPE,
        items=config.ITEMS_TO_ANALYZE
    )
    
    if not analyzer.keywords:
        return None

    # Run the initial keyword scan
    processed_df = analyzer.process_filings()
    if processed_df.empty:
        return None
        
    # Save the raw scan results before filtering
    raw_scan_filename = os.path.join(config.OUTPUT_DIR, f"{config.FILENAME_PREFIX}_Raw_Keyword_Scan.csv")
    processed_df.to_csv(raw_scan_filename, index=False, encoding='utf-8-sig')
    print(f"Raw keyword scan results saved to '{raw_scan_filename}'")

    # Filter for the specific firms of interest
    relevant_firms_df = filter_relevant_firms(processed_df)
    
    if relevant_firms_df.empty:
        print("No relevant firms found after filtering.")
        return None

    # Sort and save the final list of relevant firms
    relevant_firms_df.sort_values(by='cik', inplace=True)
    output_filename = os.path.join(config.OUTPUT_DIR, f"{config.FILENAME_PREFIX}_Relevant_Firms.csv")
    
    # Drop the full text columns before saving the final list
    text_cols_to_drop = [col for col in relevant_firms_df.columns if col.endswith('_text')]
    relevant_firms_df.drop(columns=text_cols_to_drop, inplace=True)
    
    relevant_firms_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"\nStep 1 Complete: Found {len(relevant_firms_df)} relevant firms.")
    print(f"Final list saved to '{output_filename}'")
    
    return output_filename

if __name__ == '__main__':
    # This allows the script to be run standalone for testing
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
    run_keyword_analysis()
