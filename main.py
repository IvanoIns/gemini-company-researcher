import os
import time
import logging
import unicodedata
import re
import pandas as pd
from tqdm.auto import tqdm
import pycountry
import regex
import colorama
from colorama import Fore, Style
from difflib import SequenceMatcher
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- USER CONFIGURATION ---
# Define the research focus and keywords here.
# This is the only section you need to modify to adapt the script for a new research topic.

# Example 1: Original Industrial Automation Focus
# RESEARCH_FOCUS_DESCRIPTION = "industrial automation components, such as HMIs (Human Machine Interfaces) or IPCs (Industrial PCs)"
# RELEVANCE_KEYWORDS = ["uses hmi", "uses ipc", "integrates hmi", "factory automation", "machine control", "manufacturer"]

# Example 2: Researching companies that use specific cloud services
# RESEARCH_FOCUS_DESCRIPTION = "cloud computing services, particularly AWS for data warehousing and machine learning"
# RELEVANCE_KEYWORDS = ["aws user", "cloud infrastructure", "data lake", "sagemaker", "redshift", "big data analytics"]

# Example 3: Researching companies in the sustainable packaging industry
RESEARCH_FOCUS_DESCRIPTION = "sustainable and biodegradable packaging solutions for consumer goods"
RELEVANCE_KEYWORDS = ["sustainable packaging", "biodegradable materials", "eco-friendly packaging", "circular economy", "consumer goods"]


# --- SCRIPT CONSTANTS  ---
GEMINI_MODEL_NAME = "gemini-2.5-flash" # <<< USE YOUR CHOICE MODEL
CSV_FILE_PATH = r"data/sample_companies.csv"  # <<< POINT THIS TO YOUR INPUT CSV FILE
DELAY = 2
MAX_RETRIES = 2
RETRY_DELAY = 15
SIMILARITY_THRESHOLD = 0.50
ASIAN_COMPANY_THRESHOLD = 0.40
MIN_DESCRIPTION_LENGTH = 50
MINIMUM_ACCEPTABLE_SCORE_FOR_RESEARCH_TARGET = 0.4
# --- END OF CONFIGURATION ---


# Securely get the API key
API_KEY_SET_SUCCESSFULLY = False
if os.getenv("GOOGLE_API_KEY"):
    API_KEY_SET_SUCCESSFULLY = True
    print(f"{Fore.GREEN}Successfully loaded GOOGLE_API_KEY from .env file.{Style.RESET_ALL}")
else:
    print(f"{Fore.RED}CRITICAL: GOOGLE_API_KEY not found in .env file or environment variables.{Style.RESET_ALL}")

colorama.init(autoreset=True)

VERBOSE = True
logger = None

# --- MODIFIED PRINT HELPER FUNCTIONS ---
def print_message_with_log(message, level="info", color_prefix=""):
    log_message = str(message)
    if VERBOSE:
        formatted_msg = f"{color_prefix}{log_message}{Style.RESET_ALL if color_prefix else ''}"
        print(formatted_msg)

    if logger:
        clean_log_message = re.sub(r'\x1b\[[0-9;]*[mK]', '', log_message)
        if level == "info": logger.info(clean_log_message)
        elif level == "warning": logger.warning(clean_log_message)
        elif level == "error": logger.error(clean_log_message)
        elif level == "critical": logger.critical(clean_log_message)
        else: logger.log(getattr(logging, level.upper(), logging.INFO), clean_log_message)

def print_info(message): print_message_with_log(message, level="info", color_prefix=Fore.BLUE)
def print_success(message): print_message_with_log(message, level="info", color_prefix=Fore.GREEN)
def print_warning(message): print_message_with_log(message, level="warning", color_prefix=Fore.YELLOW)
def print_error(message): print_message_with_log(message, level="error", color_prefix=Fore.RED)


try:
    import google.generativeai as genai
    from google.generativeai import types as google_genai_types
except ImportError:
    print_error("CRITICAL ERROR: The 'google-generativeai' SDK is not found. "
                "Please ensure it's installed correctly with 'pip install google-generativeai --upgrade'.")
    exit(1)

import google.api_core.exceptions

def print_section(title):
    section_bar_char = '='
    try:
        width = os.get_terminal_size().columns
    except (OSError, AttributeError):
        width = 80
    section_bar_str = section_bar_char * width
    title_max_len_display = width - (len(section_bar_char * 2) * 2 + 2)
    if title_max_len_display < 0: title_max_len_display = 0
    truncated_title_display = title if len(title) <= title_max_len_display else (title[:title_max_len_display-3] + "..." if title_max_len_display >= 3 else title[:title_max_len_display])
    console_formatted_title_str = f"{section_bar_char*2} {truncated_title_display} {section_bar_char*2}"

    if VERBOSE:
        section_output_str = (
            f"\n{Fore.CYAN}{section_bar_str}{Style.RESET_ALL}\n"
            f"{Fore.CYAN}{console_formatted_title_str}{Style.RESET_ALL}\n"
            f"{Fore.CYAN}{section_bar_str}{Style.RESET_ALL}"
        )
        print(section_output_str)

    if logger:
        log_title_formatted_str = f"{section_bar_char*2} {title} {section_bar_char*2}"
        logger.info(f"\n{section_bar_str}")
        logger.info(log_title_formatted_str)
        logger.info(section_bar_str)

# --- Output Directory and Logging Setup ---
if os.path.isabs(CSV_FILE_PATH) and os.path.dirname(CSV_FILE_PATH):
    OUTPUT_DIR = os.path.dirname(CSV_FILE_PATH)
else:
    OUTPUT_DIR = os.getcwd()
    if not os.path.isabs(CSV_FILE_PATH):
        CSV_FILE_PATH = os.path.join(OUTPUT_DIR, os.path.basename(CSV_FILE_PATH))

timestamp = time.strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE_BASENAME = f'structured_company_entities_gemini_{GEMINI_MODEL_NAME.replace("/", "_").replace("-preview-","_")}_{timestamp}'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'{OUTPUT_FILE_BASENAME}.xlsx')
LOG_FILE = os.path.join(OUTPUT_DIR, f'{OUTPUT_FILE_BASENAME}.log')

logging.basicConfig(handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8')],
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- UPDATED, NEUTRAL FIELD NAMES ---
PHASE_1_OUTPUT_FIELDS = [
    "Input Company Name Query", "Input Country Query",
    "Entity_Official_Company_Name", "Entity_Headquarters_Location",
    "Entity_Primary_Industry_Sector", "Entity_Detailed_Business_Activities_Model",
    "Entity_Key_Products_Services_Categorized",
    "Relevance_to_Research_Focus",
    "Entity_Technology_Focus_Specializations",
    "Entity_Target_Customer_Segments", "Entity_Website",
    "Entity_Disambiguation_Notes", "Entity_Block_Match_Confidence",
    "Entity_Block_Is_Likely_Match", "Original_LLM_Response_Block_Snippet"
]


def configure_gemini():
    if not API_KEY_SET_SUCCESSFULLY:
        print_error("Cannot configure Gemini: API key not set.")
        return None
    try:
        genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
        print_success("genai.configure() successful.")
        logger.info("genai.configure() successful.")
        return genai
    except Exception as e:
        print_error(f"Failed to configure Gemini: {e}. Ensure GOOGLE_API_KEY is valid.")
        logger.critical(f"Failed to configure Gemini: {e}")
        return None

gemini_sdk = configure_gemini()

def sanitize_company_name(company_name):
    if not isinstance(company_name, str): company_name = str(company_name)
    normalized_name = unicodedata.normalize('NFKC', company_name)
    sanitized_name = regex.sub(r'[^\p{L}\p{N}\s\.\-\&\'㈜]', ' ', normalized_name)
    sanitized_name = re.sub(r'\s+', ' ', sanitized_name).strip()
    return sanitized_name

def is_asian_company(country_name):
    if not isinstance(country_name, str): return False
    asian_countries = ["china", "hong kong", "taiwan", "japan", "korea", "south korea"]
    return country_name.lower() in asian_countries

def gemini_api_call(prompt_content, system_instruction_text, model_name_param=GEMINI_MODEL_NAME, retries_param=MAX_RETRIES):
    if not gemini_sdk:
        print_error("Gemini SDK not initialized. Skipping API call.")
        logger.error("Gemini SDK not initialized. Skipping API call.")
        return None

    model = gemini_sdk.GenerativeModel(
        model_name=model_name_param,
        system_instruction=system_instruction_text,
        generation_config={'temperature': 0.1, 'top_p': 0.95},
        safety_settings=[
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ],
        tools=['google_search']
    )

    current_retry_delay_val = RETRY_DELAY
    print_info(f"Sending request to Gemini API ({model_name_param})... Prompt: {str(prompt_content)[:100]}...")
    logger.info(f"Sending request to Gemini API ({model_name_param}). Prompt: {str(prompt_content)[:100]}...")

    for attempt in range(retries_param + 1):
        try:
            response = model.generate_content(prompt_content, request_options={'timeout': 120})

            response_text = ""
            if hasattr(response, 'text') and response.text:
                response_text = response.text
            elif hasattr(response, 'parts') and response.parts:
                response_text = "".join(part.text for part in response.parts if hasattr(part, 'text'))

            if hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
                reason = response.prompt_feedback.block_reason
                print_error(f"Gemini prompt blocked (Attempt {attempt + 1}). Reason: {reason}")
                logger.error(f"Gemini prompt blocked. Reason: {reason}, Prompt: {str(prompt_content)[:100]}")
                return None

            if not response_text:
                print_warning(f"Gemini API returned empty text (Attempt {attempt + 1}).")
                if response.candidates and response.candidates[0].finish_reason:
                    finish_reason_str = response.candidates[0].finish_reason.name
                    print_warning(f"Candidate 0 Finish Reason: {finish_reason_str}")
                    if finish_reason_str not in ["STOP", "MAX_TOKENS"]:
                        logger.error(f"Candidate finished due to {finish_reason_str}, not retrying. Prompt: {str(prompt_content)[:100]}")
                        return None
            else:
                print_success(f"Gemini API call successful (attempt {attempt + 1}).")
                logger.info(f"Gemini API call successful for prompt: {str(prompt_content)[:100]}")
                return response_text

        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.ServiceUnavailable) as e:
            print_error(f"Gemini API unavailable or rate limited (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Gemini API error: {e} for prompt {str(prompt_content)[:100]}")
        except Exception as e:
            print_error(f"Unexpected error during Gemini API call (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Unexpected error: {e} for prompt {str(prompt_content)[:100]}")

        if attempt < retries_param:
            print_warning(f"Retrying in {current_retry_delay_val} seconds...")
            time.sleep(current_retry_delay_val)
            current_retry_delay_val *= 2
        else:
            logger.error(f"Max retries ({retries_param}) reached for Gemini API call. Prompt: {str(prompt_content)[:100]}")
            return None
    return None

def clean_llm_response(response):
    if not response: return None
    text = str(response)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'```(?:json)?\s*\n?(.*?)\n?```', r"\1", text, flags=re.DOTALL | re.IGNORECASE)
    cleaned_lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(cleaned_lines)

def verify_company_match(text_block_for_verification, original_queried_name, country_name_queried=None):
    if not text_block_for_verification: return False, 0.0
    parsed_name_from_block = None
    name_match = re.search(r"Official Company Name:\s*([^\n]+)", text_block_for_verification, re.IGNORECASE)
    if name_match: parsed_name_from_block = name_match.group(1).strip()
    
    if not parsed_name_from_block: return False, 0.05

    sanitized_original_query = sanitize_company_name(original_queried_name)
    sanitized_parsed_name = sanitize_company_name(parsed_name_from_block)
    
    if not sanitized_original_query or not sanitized_parsed_name: return False, 0.0
    
    name_similarity_score = SequenceMatcher(None, sanitized_original_query.lower(), sanitized_parsed_name.lower()).ratio()
    
    location_similarity_score = 0.0
    country_match_strictness = 0.0
    
    parsed_location_from_block = None
    loc_match = re.search(r"Headquarters Location:\s*([^\n]+)", text_block_for_verification, re.IGNORECASE)
    if loc_match: parsed_location_from_block = loc_match.group(1).strip()

    if parsed_location_from_block and country_name_queried:
        if country_name_queried.lower() in parsed_location_from_block.lower():
            country_match_strictness = 1.0
            location_similarity_score = 1.0
    
    combined_confidence = (0.7 * name_similarity_score) + (0.3 * location_similarity_score)
    
    current_threshold = ASIAN_COMPANY_THRESHOLD if country_name_queried and is_asian_company(country_name_queried) else SIMILARITY_THRESHOLD
    return combined_confidence >= current_threshold, combined_confidence

def get_company_info(company_name, country_name):
    print_section(f"RESEARCHING (Gemini {GEMINI_MODEL_NAME}): '{company_name}' ({country_name})")
    safe_company_name_query = sanitize_company_name(company_name)

    # --- GENERALIZED PROMPTS ---
    system_prompt_for_research = "You are an expert business researcher and analyst. Your primary goal is to provide accurate, structured information about specific business entities based on a defined research focus."

    user_prompt_for_research = (
        f"The company '{safe_company_name_query}' (Original queried name: '{company_name}') is potentially relevant to the following research topic: **{RESEARCH_FOCUS_DESCRIPTION}**.\n"
        f"Your task is to identify and provide detailed information for **this specific business entity** that is headquartered or has its principal operations in **{country_name}**.\n\n"
        "CRITICAL FOCUS:\n"
        "1.  **Identify the Correct Entity:** Prioritize the legal entity that matches '{safe_company_name_query}' and is relevant to the research topic within **{country_name}**. Distinguish it from companies with similar names in unrelated sectors.\n"
        "2.  **Country Specificity:** The primary operations and headquarters of the entity you detail MUST be in **{country_name}**.\n"
        "3.  **Current Operational Status:** If the entity was acquired or merged, identify its current operational successor in **{country_name}**. Clearly note this transition in 'Disambiguation Notes'.\n\n"
        "INSTRUCTIONS FOR OUTPUT:\n"
        "A. IF YOU FIND THE SPECIFIC TARGET ENTITY (or its direct successor) IN **{country_name}**:\n"
        "   Present its information in a section starting with '--- ENTITY START ---'.\n"
        "   Provide details using these field labels ON SEPARATE LINES:\n"
        "       Official Company Name: [Full Legal or commonly used English Name for THIS entity in **{country_name}**]\n"
        "       Original Script Name: [Name in original language for THIS entity in **{country_name}**, if applicable. Else 'N/A'.]\n"
        "       Headquarters Location: [City, **{country_name}** for THIS entity.]\n"
        "       Website: [Official website for THIS entity.]\n"
        "       Primary Industry/Sector: [Main industry of THIS entity.]\n"
        f"      Detailed Business Activities & Model: [Describe what THIS entity makes or does in **{country_name}**, focusing on aspects relevant to the research topic: **{RESEARCH_FOCUS_DESCRIPTION}**.]\n"
        "       Key Products/Services (Categorized): [Main offerings of THIS entity.]\n"
        f"      Relevance to Research Focus: [REQUIRED: Based on your research, explain HOW this specific entity is relevant to **{RESEARCH_FOCUS_DESCRIPTION}**. For example: 'The company manufactures biodegradable polymers for food packaging.' or 'They are a key user of AWS Redshift for their analytics platform.']\n"
        "       Technology Focus / Key Specializations: [Key technologies relevant to THIS entity's operations.]\n"
        "       Target Customer Segments: [Main customer types for THIS entity.]\n"
        "       Disambiguation Notes: [Clarify if this is the correct entity, a successor, or if there are other similarly named companies. Note any name variations from the query.]\n\n"
        "B. IF MULTIPLE DISTINCT ENTITIES IN **{country_name}** ARE STRONG CANDIDATES:\n"
        "   Provide separate '--- ENTITY START ---' sections for each, detailing why each is a plausible match.\n\n"
        "C. IF THE SPECIFIC ENTITY CANNOT BE CLEARLY IDENTIFIED OR CONFIRMED AS RELEVANT IN **{country_name}**:\n"
        "   State: 'TARGET_ENTITY_NOT_CLEARLY_IDENTIFIED_IN_COUNTRY: Unable to definitively identify or confirm '{safe_company_name_query}' in **{country_name}** as a relevant entity based on available information.'\n\n"
        "Ensure your entire response adheres to one of these output structures."
    )

    llm_full_response = gemini_api_call(user_prompt_for_research, system_prompt_for_research)
    cleaned_llm_response = clean_llm_response(llm_full_response)

    structured_entities_found_from_llm = []
    if not cleaned_llm_response or "TARGET_ENTITY_NOT_CLEARLY_IDENTIFIED_IN_COUNTRY" in cleaned_llm_response.upper():
        logger.warning(f"Gemini indicated NO_SPECIFIC_ENTITY_FOUND for {company_name} in {country_name}.")
    else:
        entity_text_blocks = [block.strip() for block in re.split(r"--- ENTITY START ---", cleaned_llm_response, flags=re.IGNORECASE) if block.strip()]
        if not entity_text_blocks and "Official Company Name:" in cleaned_llm_response:
            entity_text_blocks = [cleaned_llm_response]
        
        print_info(f"Found {len(entity_text_blocks)} potential entity blocks in Gemini response.")

        for i, block_text in enumerate(entity_text_blocks):
            if len(block_text) < MIN_DESCRIPTION_LENGTH: continue
            
            entity_data = {"Input Company Name Query": company_name, "Input Country Query": country_name, **{f: "Not parsed" for f in PHASE_1_OUTPUT_FIELDS if f not in ["Input Company Name Query", "Input Country Query"]}, "Original_LLM_Response_Block_Snippet": block_text[:3000]}
            
            # --- UPDATED PARSING MAP ---
            parsing_labels_map = {
                "Official Company Name": "Entity_Official_Company_Name",
                "Original Script Name": "Original Script Name",
                "Headquarters Location": "Entity_Headquarters_Location",
                "Website": "Entity_Website",
                "Primary Industry/Sector": "Entity_Primary_Industry_Sector",
                "Detailed Business Activities & Model": "Entity_Detailed_Business_Activities_Model",
                "Key Products/Services (Categorized)": "Entity_Key_Products_Services_Categorized",
                "Relevance to Research Focus": "Relevance_to_Research_Focus",
                "Technology Focus / Key Specializations": "Entity_Technology_Focus_Specializations",
                "Target Customer Segments": "Entity_Target_Customer_Segments",
                "Disambiguation Notes": "Entity_Disambiguation_Notes"
            }
            
            for label_in_prompt, dict_key_in_entity_data in parsing_labels_map.items():
                if not is_asian_company(country_name) and label_in_prompt == "Original Script Name": continue
                other_labels_escaped = [re.escape(l_prompt) for l_prompt in parsing_labels_map.keys() if l_prompt != label_in_prompt]
                lookahead = r"(?=\n\s*(?:" + "|".join(other_labels_escaped) + r"):\s*|$)"
                match = re.search(rf"^\s*{re.escape(label_in_prompt)}:\s*(.*?){lookahead}", block_text, re.MULTILINE | re.DOTALL | re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    entity_data[dict_key_in_entity_data] = value if value and value.lower() not in ["n/a", "-"] else "Not specified"
            
            text_for_verification = f"Official Company Name: {entity_data.get('Entity_Official_Company_Name', '')}\nHeadquarters Location: {entity_data.get('Entity_Headquarters_Location', '')}"
            is_block_match, block_confidence = verify_company_match(text_for_verification, company_name, country_name)
            entity_data["Entity_Block_Match_Confidence"] = f"{block_confidence:.2f}"
            entity_data["Entity_Block_Is_Likely_Match"] = "Yes" if is_block_match else "No"
            
            if entity_data.get("Entity_Official_Company_Name", "Not parsed").lower() not in ["not parsed", "not specified"]:
                structured_entities_found_from_llm.append(entity_data)
                print_success(f"Parsed potential entity: '{entity_data['Entity_Official_Company_Name']}' with conf {block_confidence:.2f}")

    final_selected_entity_list = []
    if structured_entities_found_from_llm:
        candidate_entities = []
        for entity_dict in structured_entities_found_from_llm:
            if country_name.lower() not in entity_dict.get("Entity_Headquarters_Location", "").lower():
                print_warning(f"Post-filter: Skipping '{entity_dict.get('Entity_Official_Company_Name')}' (location mismatch).")
                continue
            
            relevance_text = entity_dict.get("Relevance_to_Research_Focus", "n/a").lower()
            relevance_score = 0.5 if len(relevance_text) > 10 and "not applicable" not in relevance_text else 0.0
            if any(kw in relevance_text for kw in RELEVANCE_KEYWORDS):
                relevance_score = 1.0
            
            name_loc_conf = float(entity_dict.get("Entity_Block_Match_Confidence", "0.0"))
            final_score = (name_loc_conf * 0.4) + (relevance_score * 0.6)
            
            if final_score > MINIMUM_ACCEPTABLE_SCORE_FOR_RESEARCH_TARGET:
                candidate_entities.append({"entity_data": entity_dict, "final_score": final_score})
            else:
                print_info(f"Post-filter: Entity '{entity_dict.get('Entity_Official_Company_Name')}' rejected. Score: {final_score:.2f}")
            
        if candidate_entities:
            best_candidate_data = sorted(candidate_entities, key=lambda x: x["final_score"], reverse=True)[0]['entity_data']
            print_success(f"Selected best entity in {country_name}: '{best_candidate_data['Entity_Official_Company_Name']}' (Score: {sorted(candidate_entities, key=lambda x: x['final_score'], reverse=True)[0]['final_score']:.2f})")
            final_selected_entity_list = [best_candidate_data]
            
    if not final_selected_entity_list:
        logger.warning(f"No suitable entity profile identified for '{company_name}' in '{country_name}'.")
        return [{"Input Company Name Query": company_name, "Input Country Query": country_name, "Entity_Official_Company_Name": "No specific entity profile identified", **{field: "Processing failed to identify target" for field in PHASE_1_OUTPUT_FIELDS if field not in ["Input Company Name Query", "Input Country Query", "Entity_Official_Company_Name"]}, "Original_LLM_Response_Block_Snippet": cleaned_llm_response[:5000] if cleaned_llm_response else "No response from LLM.", "Entity_Block_Match_Confidence": "0.00", "Entity_Block_Is_Likely_Match": "No"}]
        
    return final_selected_entity_list

def save_results(results_list_for_excel, output_file_path, columns_to_write):
    try:
        if not results_list_for_excel: print_warning("No results to save."); return True
        df = pd.DataFrame(results_list_for_excel)
        df = df.reindex(columns=columns_to_write)
        df.to_excel(output_file_path, index=False, engine='openpyxl')
        print_success(f"Results ({len(df)} rows) saved to {output_file_path}")
        logger.info(f"Results ({len(df)} rows) saved to {output_file_path}")
        return True
    except Exception as e:
        print_error(f"Error saving results to Excel: {e}")
        logger.error(f"Error saving results to Excel {output_file_path}: {e}")
        return False

def process_company(company_name_query, country_name_query, all_results_accumulator_list):
    list_of_found_entities = get_company_info(company_name_query, country_name_query)
    for entity_data_dict in list_of_found_entities:
        all_results_accumulator_list.append(entity_data_dict)
    save_results(all_results_accumulator_list, OUTPUT_FILE, PHASE_1_OUTPUT_FIELDS)

def process_companies_from_csv(csv_file_path_param):
    print_section(f"STARTING COMPANY PROCESSING - GEMINI ({GEMINI_MODEL_NAME})")

    try:
        df_input = pd.read_csv(csv_file_path_param, encoding='utf-8')
    except Exception as e:
        print_error(f"Error reading CSV file {csv_file_path_param}: {e}"); return []

    all_results_for_excel = []
    processed_input_queries_set = set()

    if os.path.exists(OUTPUT_FILE):
        try:
            print_info(f"Found existing output file: {OUTPUT_FILE}. Loading processed queries.")
            existing_df = pd.read_excel(OUTPUT_FILE)
            all_results_for_excel.extend(existing_df.to_dict('records'))
            processed_input_queries_set.update(
                (str(row["Input Company Name Query"]), str(row["Input Country Query"]))
                for _, row in existing_df.iterrows()
            )
            print_success(f"Loaded {len(all_results_for_excel)} existing records.")
        except Exception as e:
            print_warning(f"Could not load existing results from {OUTPUT_FILE}: {e}.")
            all_results_for_excel = []
    
    queries_to_run_list = [
        (str(row.get('company_name', '')).strip(), str(row.get('country', '')).strip())
        for _, row in df_input.iterrows()
        if (str(row.get('company_name', '')).strip(), str(row.get('country', '')).strip()) not in processed_input_queries_set
        and str(row.get('company_name', '')).strip() and str(row.get('country', '')).strip()
    ]

    if not queries_to_run_list:
        print_info("All companies from input CSV appear to be processed. Nothing new to run.")
        return all_results_for_excel

    print_info(f"Found {len(queries_to_run_list)} new company queries to process.")

    with tqdm(total=len(queries_to_run_list), desc="Overall Query Progress", unit="query") as overall_progress_bar:
        for i, (company_name_to_query, country_name_to_query) in enumerate(queries_to_run_list):
            overall_progress_bar.set_description(f"Processing: {company_name_to_query[:25]}...")
            process_company(company_name_to_query, country_name_to_query, all_results_for_excel)
            overall_progress_bar.update(1)
            if i < len(queries_to_run_list) - 1:
                time.sleep(DELAY)

    return all_results_for_excel

def main():
    print_section(f"COMPANY RESEARCH SCRIPT (GEMINI {GEMINI_MODEL_NAME})")
    logger.info(f"Script started. Model: {GEMINI_MODEL_NAME}. Output: {OUTPUT_FILE}.")

    if not API_KEY_SET_SUCCESSFULLY or not gemini_sdk:
        print_error("CRITICAL: API Key or Gemini SDK not configured. Exiting.")
        logger.critical("CRITICAL: API Key or Gemini SDK not configured. Exiting.")
        return
    if not os.path.exists(CSV_FILE_PATH):
         msg = f"CRITICAL: Input CSV file not found: {CSV_FILE_PATH}"; print_error(msg); logger.critical(msg); return

    results = process_companies_from_csv(CSV_FILE_PATH)

    print_section("PROCESSING COMPLETE")
    if results:
        final_df = pd.DataFrame(results)
        identified_count = len(final_df[final_df["Entity_Official_Company_Name"] != "No specific entity profile identified"])
        print_success(f"Research completed. Identified {identified_count} entities.")
        print_info(f"Final results saved to {OUTPUT_FILE}")
    else:
        print_warning("Research completed, but no new results were generated.")

if __name__ == "__main__":
    main()
