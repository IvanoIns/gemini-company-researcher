# gemini-company-researcher


# AI-Powered Company Profile Researcher

## 1. Problem Statement

Manually researching and compiling detailed profiles for hundreds of companies is a time-consuming and error-prone process for market analysis and sales teams. 
This project automates company research by leveraging the Google Gemini API to take a simple list of company names and countries, and returns a structured Excel file with detailed, verified information.

---

## 2. Key Features

* **AI-Powered Research:** Uses the Google Gemini models with Google Search grounding to find and synthesize company information.
* **Structured Data Extraction:** Parses the AI's natural language response into a structured format with fields like official name, headquarters, business activities and relevance to your industry/business.
* **Automated Confidence Scoring:** Implements a custom algorithm to score the confidence of a match based on name and location similarity, ensuring data quality.
* **Resilient & Resumable:** Includes error handling, automated retries with exponential backoff for API calls, and the ability to resume processing from an existing output file to save time and cost.
* **Secure:** Safely manages API keys using environment variables, preventing exposure of sensitive credentials.

---

## 3. Setup & Configuration

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git)
    cd YOUR_REPO_NAME
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set Up Your API Key:**
    * Rename the file `.env.example` to `.env`. (You should create a `.env.example` file that looks like your `.env` but with a placeholder key).
    * Open the `.env` file and add your Google API Key:
        ```
        GOOGLE_API_KEY="AIza...your...secret...key"
        ```

---

## 4. Usage

1.  Prepare your input data in a CSV file with `company_name` and `country` columns. See `/data/sample_companies.csv` for an example.
2.  Update the `CSV_FILE_PATH` variable in `main.py` to point to your input file.
3.  Run the script:
    ```bash
    python main.py
    ```
4.  A timestamped `.xlsx` file and a `.log` file will be generated in the same directory.

---

## 5. Technology Stack

* **Core Language:** Python
* **AI & APIs:** Google Gemini SDK (`google-genai`)
* **Data Manipulation:** Pandas
* **Utilities:** PyCountry, TQDM, Colorama, Python-Dotenv
