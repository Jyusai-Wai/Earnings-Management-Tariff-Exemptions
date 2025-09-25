import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import config

def calculate_financial_variables(raw_panel_data_file: str):
    """
    Main function for Step 3. Reads the raw panel data, sorts it,
    and calculates lagged variables, deltas, and total accruals.

    Args:
        raw_panel_data_file (str): The path to the raw panel data CSV from Step 2.

    Returns:
        str: The path to the processed data file with all calculated variables, or None if failed.
    """
    print(f"\n--- Starting Step 3: Calculate Financial Variables ---")
    
    try:
        df = pd.read_csv(raw_panel_data_file)
        df['datadate'] = pd.to_datetime(df['datadate'])
        print(f"Successfully read {len(df)} records from '{raw_panel_data_file}'.")
    except FileNotFoundError:
        print(f"❌ Error: Input file not found: '{raw_panel_data_file}'.")
        return None

    # Fill all missing numerical values with 0
    print("Filling missing values (NaN) with 0...")
    for col in df.select_dtypes(include=np.number).columns:
        df[col] = df[col].fillna(0)
    
    # Ensure data is sorted correctly before calculating lags and deltas
    sort_keys = ['gvkey', 'fyear', 'fqtr']
    print(f"Sorting data by {sort_keys}...")
    df.sort_values(by=sort_keys, ascending=True, inplace=True)

    # --- Calculate Lagged and Delta Variables ---
    vars_to_process = [
        'assets', 'liabilities', 'long_term_debt', 'current_portion_lt_debt',
        'income_taxes_payable', 'depreciation_amortization_expense',
        'cash_flow_operations', 'net_income', 'revenue', 'ppe_gross',
        'current_assets', 'cash_and_equivalents', 'current_liabilities', 'pre_tax_income'
    ]
    
    # Filter list to only variables present in the DataFrame
    vars_in_df = [v for v in vars_to_process if v in df.columns]

    print("Calculating lagged (L1) and delta (D) variables...")
    for var in tqdm(vars_in_df, desc="Processing Variables"):
        df[f"{var}_L1"] = df.groupby('gvkey')[var].shift(1)
        df[f"{var}_D"] = df[var] - df[f"{var}_L1"]

    # --- Calculate Total Accruals (TA) ---
    print("Calculating Total Accruals (TA)...")
    required_ta_cols = {
        'current_assets_D', 'cash_and_equivalents_D', 'current_liabilities_D',
        'current_portion_lt_debt_D', 'income_taxes_payable_D', 'depreciation_amortization_expense'
    }
    
    if required_ta_cols.issubset(df.columns):
        df['TA'] = (df['current_assets_D'] - df['cash_and_equivalents_D']) - \
                   (df['current_liabilities_D'] - df['current_portion_lt_debt_D'] - df['income_taxes_payable_D']) - \
                   df['depreciation_amortization_expense']
        
        # Calculate lag and delta for TA as well
        df['TA_L1'] = df.groupby('gvkey')['TA'].shift(1)
        df['TA_D'] = df['TA'] - df['TA_L1']
        print("TA and its lagged/delta variables calculated successfully.")
    else:
        print("❌ Error: Not all required components for TA calculation were found. Skipping TA calculation.")
        return None

    output_filename = os.path.join(config.OUTPUT_DIR, f"{config.FILENAME_PREFIX}_Processed_Panel_Data.csv")
    
    try:
        df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\n✅ Step 3 Complete! Processed data with all calculated variables saved to '{output_filename}'.")
        return output_filename
    except Exception as e:
        print(f"❌ Error saving processed file: {e}")
        return None

if __name__ == '__main__':
    # Standalone test execution
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
    test_raw_data_file = os.path.join(config.OUTPUT_DIR, f"{config.FILENAME_PREFIX}_Raw_Panel_Data.csv")
    if os.path.exists(test_raw_data_file):
        calculate_financial_variables(test_raw_data_file)
    else:
        print(f"Test run failed: Prerequisite file not found at '{test_raw_data_file}'")
