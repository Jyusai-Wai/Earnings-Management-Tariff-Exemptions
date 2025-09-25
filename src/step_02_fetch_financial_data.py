import pandas as pd
import numpy as np
import wrds
import os
import config

def fetch_financial_data(relevant_firms_file: str):
    """
    Main function for Step 2. Connects to WRDS, links CIKs to GVKEYs,
    and downloads a raw quarterly panel dataset for the specified firms.

    Args:
        relevant_firms_file (str): The path to the CSV file containing the list
                                   of relevant firms from Step 1.

    Returns:
        str: The path to the saved raw panel data CSV file, or None if failed.
    """
    print("\n--- Starting Step 2: Fetch Financial Data from WRDS ---")
    db = None
    try:
        # --- 1. Connect to WRDS Database ---
        print("Connecting to WRDS...")
        db = wrds.Connection(wrds_username=config.WRDS_USERNAME)
        print("WRDS connection successful.")

        # --- 2. Prepare CIK list from relevant firms file ---
        print(f"Preparing CIK list from '{relevant_firms_file}'...")
        try:
            filtered_firms = pd.read_csv(relevant_firms_file)
            name_lookup = filtered_firms[['cik', 'company_name']].drop_duplicates()
            name_lookup['cik'] = name_lookup['cik'].astype(str).str.zfill(10)
            cik_list = name_lookup['cik'].unique().tolist()
            if not cik_list: raise ValueError("CIK list is empty.")
            print(f"Prepared {len(cik_list)} unique CIKs for processing.")
        except (FileNotFoundError, KeyError, ValueError) as e:
            print(f"❌ Error processing relevant firms file: {e}")
            return None

        # --- 3. Link CIKs to GVKEYs using Compustat ---
        print("Linking CIKs to GVKEYs...")
        all_companies = db.get_table(library='comp', table='company', columns=['gvkey', 'cik'])
        all_companies['cik'] = all_companies['cik'].astype(str).str.zfill(10)
        matched_companies = all_companies[all_companies['cik'].isin(cik_list)].copy()
        gvkey_list = matched_companies['gvkey'].unique().tolist()
        if not gvkey_list:
            print("❌ Error: No GVKEYs found for the provided CIKs.")
            return None
        print(f"Successfully linked to {len(gvkey_list)} unique GVKEYs.")

        # --- 4. Define and Download Raw Quarterly Financial Data ---
        end_date_fd = f"{config.GLOBAL_PREDICTION_END_YEAR}-12-31"
        start_date_fd = f"{config.GLOBAL_PREDICTION_END_YEAR - config.FINANCIAL_DATA_YEARS_TO_DOWNLOAD}-01-01"
        print(f"Downloading quarterly data from {start_date_fd} to {end_date_fd}...")

        compustat_vars = [
            'gvkey', 'datadate', 'fyearq', 'fqtr', 'atq', 'ltq', 'dlttq', 'dlcq',
            'txpdy', 'dpq', 'oancfy', 'niq', 'saleq', 'ppegtq', 'actq', 'cheq', 'lctq', 'piq'
        ]
        
        gvkey_tuple = tuple(gvkey_list)
        query = f"""
            SELECT {', '.join(compustat_vars)}
            FROM comp.fundq
            WHERE gvkey IN {gvkey_tuple}
            AND datadate BETWEEN '{start_date_fd}' AND '{end_date_fd}'
        """
        comp_data_raw = db.raw_sql(query)
        if comp_data_raw.empty:
            print("❌ Error: No financial data returned from Compustat for the given GVKEYs and date range.")
            return None
        print(f"Successfully downloaded {len(comp_data_raw)} raw quarterly records.")

        # --- 5. Process Raw Data ---
        df_deduped = comp_data_raw.drop_duplicates(subset=['gvkey', 'datadate'], keep='first').copy()
        
        rename_map = {
            'fyearq': 'fyear', 'atq': 'assets', 'ltq': 'liabilities', 'dlttq': 'long_term_debt',
            'dlcq': 'current_portion_lt_debt', 'txpdy': 'income_taxes_payable',
            'dpq': 'depreciation_amortization_expense', 'oancfy': 'cash_flow_operations',
            'niq': 'net_income', 'saleq': 'revenue', 'ppegtq': 'ppe_gross',
            'actq': 'current_assets', 'cheq': 'cash_and_equivalents',
            'lctq': 'current_liabilities', 'piq': 'pre_tax_income'
        }
        df = df_deduped.rename(columns=rename_map)
        df['datadate'] = pd.to_datetime(df['datadate'])

        # --- 6. Merge Identifiers and Save ---
        panel_with_cik = pd.merge(df, matched_companies[['gvkey', 'cik']], on='gvkey', how='left')
        panel_with_names = pd.merge(panel_with_cik, name_lookup, on='cik', how='left')
        
        # Reorder columns for clarity
        cols_order = ['gvkey', 'company_name', 'cik', 'datadate', 'fyear', 'fqtr'] + \
                     [v for k, v in rename_map.items() if v != 'fyear']
        final_panel = panel_with_names.reindex(columns=[col for col in cols_order if col in panel_with_names.columns])
        
        final_panel.dropna(subset=['gvkey', 'fyear', 'fqtr'], inplace=True)

        output_filename = os.path.join(config.OUTPUT_DIR, f"{config.FILENAME_PREFIX}_Raw_Panel_Data.csv")
        final_panel.to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ Step 2 Complete! Raw quarterly panel data saved to '{output_filename}'")
        return output_filename

    except Exception as e:
        print(f"\n❌ An unexpected error occurred in Step 2: {e}")
        return None
    finally:
        if db:
            db.close()
            print("\nWRDS connection closed.")

if __name__ == '__main__':
    # Standalone test execution
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
    # Assumes a relevant firms file from Step 1 already exists in the output directory
    test_relevant_firms_file = os.path.join(config.OUTPUT_DIR, f"{config.FILENAME_PREFIX}_Relevant_Firms.csv")
    if os.path.exists(test_relevant_firms_file):
        fetch_financial_data(test_relevant_firms_file)
    else:
        print(f"Test run failed: Prerequisite file not found at '{test_relevant_firms_file}'")
