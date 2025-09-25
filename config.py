# =========================================================================
# CONFIGURATION FILE for Earnings Management Analysis Pipeline
# =========================================================================

# --- Step 1: SEC Keyword Analysis Configuration ---
# !!! IMPORTANT: Enter your personal API key from sec-api.io here !!!
SEC_API_KEY = "YOUR_SEC_API_KEY_HERE"
KEYWORD_FILE_PATH = "keywords.csv"  # Initial keyword list from external file
FORM_TYPE = "10-K"
# Items from the 10-K to analyze (e.g., '1' for Business, '1A' for Risk Factors)
ITEMS_TO_ANALYZE = ['1', '1A', '2', '7']

# Keywords used in Step 1 (Part 2) to filter for relevant firms
# These are hardcoded as they are central to the research question.
TARIFF_KEYWORDS = {"tariff", "tariffs", "duty", "duties"}
POLICY_KEYWORDS = {"trade war", "section 301", "trade policy"}
EXEMPTION_KEYWORDS = {"exemption", "exemptions", "waiver", "relief", "exclusion"}


# --- Step 2 & 3: Financial Data and Variable Calculation ---
# !!! IMPORTANT: Enter your WRDS credentials here !!!
WRDS_USERNAME = "YOUR_WRDS_USERNAME"

# The number of years of historical financial data to download prior to the analysis end year.
# e.g., 25 years of data for a 2025 analysis would go back to 2000.
FINANCIAL_DATA_YEARS_TO_DOWNLOAD = 25


# --- Step 4 & 5: Regression and Analysis Configuration ---
# The fixed "event year" for the analysis. Discretionary accruals will be
# analyzed relative to this year (e.g., Year -1 and Year 0).
FIXED_EVENT_YEAR = 2025

# Minimum number of quarterly observations required for a firm to be included in the regression sample.
# (e.g., 14 years * 4 quarters/year = 56 observations)
MIN_TOTAL_OBS_FOR_SAMPLE = 14 * 4

# Minimum number of observations required for a firm's specific OLS regression in the estimation period.
# Should be greater than the number of independent variables (which is 4).
MIN_OBS_FOR_FIRM_REGRESSION = 10

# Global date boundaries for the data used in the regression analysis.
# Estimation Period: from START to END
GLOBAL_ESTIMATION_START_YEAR = 2000
GLOBAL_ESTIMATION_END_YEAR = 2017

# Prediction Period: from START to END
GLOBAL_PREDICTION_START_YEAR = 2018
GLOBAL_PREDICTION_END_YEAR = 2025


# --- File Naming and Directory Configuration ---
# Directory to store all output files
OUTPUT_DIR = "output"

# You can customize the prefixes for the output files if you wish.
# The script will automatically append dates and other details.
FILENAME_PREFIX = f"Earnings_Management_Analysis_EventYear{FIXED_EVENT_YEAR}"
