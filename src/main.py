import os
import step_01_sec_keyword_analysis as step1
import step_02_fetch_financial_data as step2
import step_03_calculate_variables as step3
import step_04_run_regression_model as step4
import step_05_analyze_results as step5
import config

def main():
    """
    Main driver to execute the entire earnings management research pipeline.
    """
    print("--- Starting Earnings Management Research Pipeline ---")

    # Ensure the output directory exists
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
        print(f"Created output directory: {config.OUTPUT_DIR}")

    # --- Execute each step of the pipeline ---

    # Step 1: Perform textual analysis on SEC filings to find relevant firms
    relevant_firms_file = step1.run_keyword_analysis()
    if not relevant_firms_file:
        print("❌ Pipeline stopped: Step 1 did not produce a relevant firms file.")
        return

    # Step 2: Fetch quarterly financial data for the relevant firms from WRDS
    raw_panel_data_file = step2.fetch_financial_data(relevant_firms_file)
    if not raw_panel_data_file:
        print("❌ Pipeline stopped: Step 2 did not produce a raw panel data file.")
        return
        
    # Step 3: Calculate lagged variables, deltas, and total accruals
    processed_panel_data_file = step3.calculate_financial_variables(raw_panel_data_file)
    if not processed_panel_data_file:
        print("❌ Pipeline stopped: Step 3 did not produce a processed panel data file.")
        return

    # Step 4: Run the Modified Jones Model to estimate discretionary accruals
    discretionary_accruals_file, regression_summary_file = step4.run_modified_jones_model(processed_panel_data_file)
    if not discretionary_accruals_file:
        print("❌ Pipeline stopped: Step 4 did not produce a discretionary accruals file.")
        return

    # Step 5: Perform hypothesis testing and supplemental analysis on the results
    step5.analyze_regression_results(discretionary_accruals_file, regression_summary_file)

    print("\n--- ✅ Entire research pipeline completed successfully! ---")
    print(f"All results are saved in the '{config.OUTPUT_DIR}' directory.")

if __name__ == '__main__':
    main()
