# Earnings Management Behavior of Firms Seeking Tariff Exemptions

This project is an empirical financial research pipeline that investigates whether firms seeking Section 301 tariff exemptions engage in earnings management. The methodology combines textual analysis of SEC filings to identify the sample firms and econometric modeling (Modified Jones Model) to measure discretionary accruals.

## Research Pipeline Overview

The project is structured as a multi-step pipeline, executed sequentially:

1.  **Textual Analysis (SEC Filings)**:
    -   Queries `sec-api.io` to fetch 10-K filings for a specified period.
    -   Parses key sections (Items 1, 1A, 2, 7) and counts occurrences of predefined keywords related to tariffs, trade policy, and exemptions.
    -   Filters and identifies the final sample of firms that discuss all three topics, suggesting they are likely seeking tariff exemptions.

2.  **Financial Data Acquisition (WRDS)**:
    -   Takes the CIKs of the identified firms and links them to GVKEYs using Compustat.
    -   Downloads a long-panel (25+ years) of quarterly financial data from the `comp.fundq` table in WRDS.

3.  **Variable Calculation (Feature Engineering)**:
    -   Processes the raw financial data to compute lagged variables, first differences (Deltas), and Total Accruals (TA) as required by the Modified Jones Model.

4.  **Econometric Modeling (Modified Jones Model)**:
    -   For each firm, runs an OLS regression over a defined estimation period to model "normal" accruals.
    -   Uses the estimated coefficients to predict normal accruals in a subsequent prediction period.
    -   Calculates **Discretionary Accruals (DA)** as the residual from this prediction, which serves as the primary proxy for earnings management.

5.  **Analysis and Hypothesis Testing**:
    -   Performs statistical tests (Z-statistic, t-test, Wilcoxon signed-rank test) on the calculated Discretionary Accruals during the event period to test the primary hypothesis.
    -   Conducts supplemental tests, such as ANOVA on model residuals, to check for potential model misspecification.

## Project Structure

-   `config.py`: Central configuration file for API keys, dates, file paths, and keywords. **(Must be edited by the user)**
-   `requirements.txt`: A list of required Python packages.
-   `keywords.csv`: Input file with keywords for the initial SEC filing scan.
-   `src/`: Directory containing the modularized Python scripts for each step of the pipeline.
    -   `main.py`: The main driver script to execute the entire pipeline.
    -   `step_01_sec_keyword_analysis.py`: Performs textual analysis.
    -   `step_02_fetch_financial_data.py`: Downloads data from WRDS.
    -   `step_03_calculate_variables.py`: Computes financial variables.
    -   `step_04_run_regression_model.py`: Runs the Modified Jones Model.
    -   `step_05_analyze_results.py`: Conducts hypothesis testing and supplemental tests.
-   `output/`: A dedicated directory where all generated CSV and Excel files will be saved.

## How to Run

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure the Project**:
    -   **Crucial Step**: Open and edit the `config.py` file.
    -   Enter your personal `SEC_API_KEY` and WRDS credentials (`WRDS_USERNAME`).
    -   Adjust the analysis years, file paths, and keywords as needed.

3.  **Run the Pipeline**:
    -   Execute the main driver script from your terminal:
    ```bash
    python src/main.py
    ```
    -   The script will run all five steps sequentially, printing progress updates and saving intermediate and final results to the `output/` directory.
