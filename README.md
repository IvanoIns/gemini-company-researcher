# AI-Powered Company Profile Researcher

## 1. Problem Statement

Manually researching and compiling detailed company profiles is a time-consuming process for market analysis, investment research, and sales teams. This project automates company research by leveraging the Google Gemini API to take a simple list of company names and countries and return a structured Excel file with detailed, verified information based on a user-defined research focus.

---

## 2. Key Features

*   **Customizable Research:** Easily configure the script to research companies based on any industry, technology, or business model by defining a research description and keywords.
*   **AI-Powered Research:** Uses Google Gemini models with Google Search grounding to find and synthesize company information.
*   **Structured Data Extraction:** Parses the AI's natural language response into a structured format with fields like official name, headquarters, business activities, and relevance to your defined research focus.
*   **Automated Confidence Scoring:** Implements a custom algorithm to score the confidence of a match based on name and location similarity, ensuring data quality.
*   **Resilient & Resumable:** Includes error handling, automated retries for API calls, and the ability to resume processing from an existing output file to save time and cost.
*   **Secure:** Safely manages API keys using environment variables.

---

## 3. Setup

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/YOUR_USERNAME/gemini-company-researcher.git
    cd gemini-company-researcher
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set Up Your API Key:**
    *   Rename the file `.env.example` to `.env`.
    *   Open the `.env` file and add your Google API Key:
        ```
        GOOGLE_API_KEY="AIza...your...secret...key"
        ```

---

## 4. Configuration & Usage

1.  **Define Your Research Focus:**
    *   Open the `main.py` file.
    *   Navigate to the **"USER CONFIGURATION"** section at the top.
    *   Modify the `RESEARCH_FOCUS_DESCRIPTION` variable to describe the topic you are interested in (e.g., "companies developing AI-powered healthcare solutions" or "firms that utilize sustainable manufacturing processes").
    *   Update the `RELEVANCE_KEYWORDS` list with terms that the AI can use to identify relevant companies.

2.  **Prepare Your Input Data:**
    *   Create a CSV file with `company_name` and `country` columns. See `/data/sample_companies.csv` for an example.
    *   Update the `CSV_FILE_PATH` variable in `main.py` to point to your input file.

3.  **Run the Script:**
    ```bash
    python main.py
    ```

4.  **Get Results:**
    *   A timestamped `.xlsx` file and a `.log` file will be generated in the same directory as your input file.

---

## 5. Technology Stack

*   **Core Language:** Python
*   **AI & APIs:** Google Gemini SDK (`google-generativeai`)
*   **Data Manipulation:** Pandas
*   **Utilities:** PyCountry, TQDM, Colorama, Python-Dotenv
