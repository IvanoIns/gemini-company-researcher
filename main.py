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

# Securely get the API key
# The script will now fail safely if the key is not found
API_KEY_SET_SUCCESSFULLY = False
if os.getenv("GOOGLE_API_KEY"):
    # The genai library automatically uses this environment variable
    API_KEY_SET_SUCCESSFULLY = True
    print(f"{Fore.GREEN}Successfully loaded GOOGLE_API_KEY from .env file.{Style.RESET_ALL}")
else:
    print(f"{Fore.RED}CRITICAL: GOOGLE_API_KEY not found in .env file or environment variables.{Style.RESET_ALL}")

colorama.init(autoreset=True)

VERBOSE = True
logger = None

# --- MODIFIED PRINT HELPER FUNCTIONS ---
# These now use standard print() for console output, allowing the main tqdm bar to be static.
def print_message_with_log(message, level="info", color_prefix=""):
    log_message = str(message)
    if VERBOSE:
        formatted_msg = f"{color_prefix}{log_message}{Style.RESET_ALL if color_prefix else ''}"
        # REVERTED TO STANDARD PRINT for console output
        print(formatted_msg)

    if logger:
        clean_log_message = re.sub(r'\x1b\[[0-9;]*[mK]', '', log_message) # Remove color codes for log
        if level == "info": logger.info(clean_log_message)
        elif level == "warning": logger.warning(clean_log_message)
        elif level == "error": logger.error(clean_log_message)
        elif level == "critical": logger.critical(clean_log_message)
        else: logger.log(getattr(logging, level.upper(), logging.INFO), clean_log_message)

def print_info(message): print_message_with_log(message, level="info", color_prefix=Fore.BLUE)
def print_success(message): print_message_with_log(message, level="info", color_prefix=Fore.GREEN)
def print_warning(message): print_message_with_log(message, level="warning", color_prefix=Fore.YELLOW)
def print_error(message): print_message_with_log(message, level="error", color_prefix=Fore.RED)

# --- Set Environment Variable for API Key ---
API_KEY_SET_SUCCESSFULLY = False
if GOOGLE_API_KEY_VALUE and "YOUR_ACTUAL_GEMINI_API_KEY" not in GOOGLE_API_KEY_VALUE:
    os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY_VALUE
    API_KEY_SET_SUCCESSFULLY = True
    # Using standard print here as it's before the main tqdm bar
    print(f"{Fore.GREEN}Set GOOGLE_API_KEY environment variable from GOOGLE_API_KEY_VALUE.{Style.RESET_ALL}")
else:
    if not os.getenv("GOOGLE_API_KEY"):
        print(f"{Fore.RED}CRITICAL: GOOGLE_API_KEY_VALUE is a placeholder and GOOGLE_API_KEY environment variable is not set externally.{Style.RESET_ALL}")
    elif os.getenv("GOOGLE_API_KEY"):
        API_KEY_SET_SUCCESSFULLY = True
        print(f"{Fore.GREEN}Using existing GOOGLE_API_KEY environment variable.{Style.RESET_ALL}")

try:
    from google import genai
    from google.genai import types as google_genai_types
except ImportError:
    # print_error will use standard print now
    print_error("CRITICAL ERROR: The 'google-genai' SDK is not found. "
                "Please ensure it's installed correctly with 'pip install google-genai --upgrade'.")
    exit(1)

import google.api_core.exceptions

# --- Constants ---
#gemini-2.0-flash
#gemini-2.5-flash
#gemini-2.5-pro
#check https://ai.google.dev/gemini-api/docs/models for the models available on free/paid tiers

GEMINI_MODEL_NAME = "gemini-2.0-flash"
TARGET_INDUSTRY_DESCRIPTION = "industrial automation components, such as HMIs (Human Machine Interfaces) or IPCs (Industrial PCs)"
RELEVANCE_KEYWORDS = ["uses hmi", "uses ipc", "integrates hmi", "factory automation", "machine control", "manufacturer"]

CSV_FILE_PATH = r"C:\Users\SESA779789\Desktop\Data\Monthly Data\2025\new_dataset\segments_creation\perplexity_search_results\files_to_run_through_API\companies_to_find_ALL_for_gemini_5_15.csv" # <<< REPLACE THIS
DELAY = 2
MAX_RETRIES = 2
RETRY_DELAY = 15
SIMILARITY_THRESHOLD = 0.50
ASIAN_COMPANY_THRESHOLD = 0.40
MIN_DESCRIPTION_LENGTH = 50
MINIMUM_ACCEPTABLE_SCORE_FOR_CLIENT = 0.4


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

PHASE_1_OUTPUT_FIELDS = [
    "Input Company Name Query", "Input Country Query",
    "Entity_Official_Company_Name", "Entity_Headquarters_Location",
    "Entity_Primary_Industry_Sector", "Entity_Detailed_Business_Activities_Model",
    "Entity_Key_Products_Services_Categorized",
    "Industry_Relevance_Details",
    "Entity_Technology_Focus_Specializations",
    "Entity_Target_Customer_Segments", "Entity_Website",
    "Entity_Disambiguation_Notes", "Entity_Block_Match_Confidence",
    "Entity_Block_Is_Likely_Match", "Original_LLM_Response_Block_Snippet"
]

gemini_client_instance = None
try:
    if not API_KEY_SET_SUCCESSFULLY and not os.getenv("GOOGLE_API_KEY"):
        raise ValueError("GOOGLE_API_KEY environment variable not available for SDK.")
    gemini_client_instance = genai.Client()
    print_success("genai.Client() initialized successfully (should use GOOGLE_API_KEY env var).")
    logger.info("genai.Client() initialized successfully.")
except Exception as e:
    print_error(f"Failed to initialize genai.Client(): {e}. Ensure GOOGLE_API_KEY is set and valid.")
    logger.critical(f"Failed to initialize genai.Client(): {e}")

def sanitize_company_name(company_name):
    if not isinstance(company_name, str): company_name = str(company_name)
    normalized_name = unicodedata.normalize('NFKC', company_name)
    sanitized_name = regex.sub(r'[^\p{L}\p{N}\s\.\-\&\'㈜]', ' ', normalized_name)
    sanitized_name = re.sub(r'\s+', ' ', sanitized_name).strip()
    simple_name = regex.sub(r'[^\p{L}\p{N}\s]+', '', normalized_name).strip()
    return sanitized_name, simple_name

def get_primary_language(country_name):
    if not isinstance(country_name, str): country_name = str(country_name)
    country_language_map = {"China": "zh", "Hong Kong": "zh", "Taiwan": "zh", "Japan": "ja", "Korea": "ko", "South Korea": "ko", "Turkiye":"tr"}
    return country_language_map.get(country_name, "en")

def is_asian_company(country_name):
    if not isinstance(country_name, str): return False
    asian_countries = ["china", "hong kong", "taiwan", "japan", "korea", "south korea"]
    return country_name.lower() in asian_countries

def gemini_api_call(prompt_content, system_instruction_text, model_name_param=GEMINI_MODEL_NAME, retries_param=MAX_RETRIES):
    if not gemini_client_instance:
        print_error("Gemini client (genai.Client) not initialized. Skipping API call.")
        logger.error("Gemini client (genai.Client) not initialized. Skipping API call.")
        return None

    gen_config_params = {
        'temperature': 0.1,
        'top_p': 0.95,
        'tools': [
            google_genai_types.Tool(google_search=google_genai_types.GoogleSearch())
        ],
        'safety_settings': [
            google_genai_types.SafetySetting(
                category=google_genai_types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=google_genai_types.HarmBlockThreshold.BLOCK_NONE
            ), # ... (other safety settings)
             google_genai_types.SafetySetting(
                category=google_genai_types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=google_genai_types.HarmBlockThreshold.BLOCK_NONE
            ),
            google_genai_types.SafetySetting(
                category=google_genai_types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=google_genai_types.HarmBlockThreshold.BLOCK_NONE
            ),
            google_genai_types.SafetySetting(
                category=google_genai_types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=google_genai_types.HarmBlockThreshold.BLOCK_NONE
            )
        ]
    }
    if system_instruction_text:
        gen_config_params['system_instruction'] = system_instruction_text
        
    try:
        config_for_call = google_genai_types.GenerateContentConfig(**gen_config_params)
    except Exception as e_config:
        print_error(f"Error creating GenerateContentConfig: {e_config}")
        logger.error(f"Error creating GenerateContentConfig: {e_config}")
        return None
        
    current_retry_delay_val = RETRY_DELAY
   
    print_info(f"Sending request to Gemini API ({model_name_param})... Prompt: {str(prompt_content)[:100]}...")
    logger.info(f"Sending request to Gemini API ({model_name_param}). Prompt: {str(prompt_content)[:100]}...")
    
    for attempt in range(retries_param + 1):
        try:
            response = gemini_client_instance.models.generate_content(
                model=model_name_param,
                contents=prompt_content,
                config=config_for_call,
            )
            response_text = ""
            if hasattr(response, 'text') and response.text: response_text = response.text
            elif hasattr(response, 'parts') and response.parts: response_text = "".join(part.text for part in response.parts if hasattr(part, 'text'))

            if hasattr(response, 'grounding_metadata') and response.grounding_metadata: print_info(f"Grounding metadata found: {response.grounding_metadata}")
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback and hasattr(response.prompt_feedback, 'block_reason') and response.prompt_feedback.block_reason:
                reason = response.prompt_feedback.block_reason
                print_error(f"Gemini prompt blocked (Attempt {attempt + 1}). Reason: {reason}") 
                logger.error(f"Gemini prompt blocked. Reason: {reason}, Prompt: {str(prompt_content)[:100]}")
                return None 
            if not response_text:
                 print_warning(f"Gemini API returned empty text (Attempt {attempt + 1}). Feedback: {response.prompt_feedback if hasattr(response, 'prompt_feedback') else 'N/A'}")
                 if hasattr(response, 'candidates') and response.candidates and hasattr(response.candidates[0], 'finish_reason') and response.candidates[0].finish_reason:
                     finish_reason_val = response.candidates[0].finish_reason
                     finish_reason_str = str(finish_reason_val.name if hasattr(finish_reason_val, 'name') else finish_reason_val).upper()
                     print_warning(f"Candidate 0 Finish Reason: {finish_reason_str}")
                     if finish_reason_str not in ["STOP", "UNSPECIFIED", "FINISH_REASON_UNSPECIFIED", "MAX_TOKENS"]:
                         logger.error(f"Candidate finished due to {finish_reason_str}, not retrying. Prompt: {str(prompt_content)[:100]}")
                         return None 
            else:
                print_success(f"Gemini API call successful (attempt {attempt + 1}).")
                logger.info(f"Gemini API call successful for prompt: {str(prompt_content)[:100]}")
                return response_text
        
        except google.api_core.exceptions.PermissionDenied as e:
            print_error(f"Gemini API PermissionDenied (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Gemini API PermissionDenied: {e} for prompt {str(prompt_content)[:100]}")
            return None
        except google.api_core.exceptions.ResourceExhausted as e:
            print_error(f"Gemini API ResourceExhausted (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Gemini API ResourceExhausted: {e} for prompt {str(prompt_content)[:100]}")
        except google.api_core.exceptions.ServiceUnavailable as e:
            print_error(f"Gemini API ServiceUnavailable (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Gemini API ServiceUnavailable: {e} for prompt {str(prompt_content)[:100]}")
        except google.api_core.exceptions.InvalidArgument as e:
            print_error(f"Gemini API InvalidArgument (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Gemini API InvalidArgument: {e} for prompt {str(prompt_content)[:100]}")
            return None
        except google.api_core.exceptions.GoogleAPIError as e:
            print_error(f"Gemini API GoogleAPIError (Attempt {attempt + 1}/{retries_param + 1}): {e}")
            logger.error(f"Gemini API GoogleAPIError: {e} for prompt {str(prompt_content)[:100]}")
        except TypeError as te:
            print_error(f"TypeError during API call (Attempt {attempt+1}): {te}")
            logger.error(f"TypeError during API call: {te} for prompt {str(prompt_content)[:100]}")
            return None
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
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text) # Bold
    text = re.sub(r'__(.*?)__', r'\1', text) # Underline (less common from LLM)
    text = re.sub(r'\*(.*?)\*', r'\1', text)  # Italics
    text = re.sub(r'\[\d+\]', '', text) # Citations like [1], [2]
    text = re.sub(r"```(?:json)?\s*\n?(.*?)\n?```", r"\1", text, flags=re.DOTALL | re.IGNORECASE) # Code blocks
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL | re.IGNORECASE) # Thinking blocks
    cleaned_lines = [re.sub(r'[ \t]+', ' ', line.strip()) for line in text.splitlines() if line.strip()]
    return "\n".join(cleaned_lines)

def verify_company_match(text_block_for_verification, original_queried_name, country_name_queried=None):
    if not text_block_for_verification: return False, 0.0
    parsed_name_from_block = None
    name_match = re.search(r"Official Company Name:\s*([^\n]+)", text_block_for_verification, re.IGNORECASE)
    if name_match: parsed_name_from_block = name_match.group(1).strip()
    parsed_location_from_block = None
    loc_match = re.search(r"Headquarters Location:\s*([^\n]+)", text_block_for_verification, re.IGNORECASE)
    if loc_match: parsed_location_from_block = loc_match.group(1).strip()
    if not parsed_name_from_block:
        potential_name_line = text_block_for_verification.split('\n', 1)[0]
        if not any(kw in potential_name_line.lower() for kw in ["headquarters", "primary industry", "website"]):
             parsed_name_from_block = potential_name_line.strip()
        if not parsed_name_from_block: return False, 0.05
    sanitized_original_query, _ = sanitize_company_name(original_queried_name)
    sanitized_parsed_name, _ = sanitize_company_name(parsed_name_from_block)
    if not sanitized_original_query or not sanitized_parsed_name: return False, 0.0
    name_similarity_score = SequenceMatcher(None, sanitized_original_query.lower(), sanitized_parsed_name.lower()).ratio()
    location_similarity_score, country_match_strictness = 0.0, 0.0
    if parsed_location_from_block and country_name_queried:
        if country_name_queried.lower() in parsed_location_from_block.lower():
            country_match_strictness, location_similarity_score = 1.0, 1.0
        else:
            try:
                country_obj_queried = pycountry.countries.get(name=country_name_queried)
                country_obj_found = next((pycountry.countries.get(name=p.strip()) for p in parsed_location_from_block.split(',') if pycountry.countries.get(name=p.strip(), default=None)), None)
                if country_obj_queried and country_obj_found and country_obj_queried.alpha_2 == country_obj_found.alpha_2: country_match_strictness, location_similarity_score = 0.9, 0.9
                elif country_obj_queried and country_obj_found: country_match_strictness, location_similarity_score = -0.5, 0.1
                else: location_similarity_score = SequenceMatcher(None, country_name_queried.lower(), parsed_location_from_block.lower()).ratio()
            except LookupError: location_similarity_score = SequenceMatcher(None, country_name_queried.lower(), parsed_location_from_block.lower()).ratio()
    elif country_name_queried: location_similarity_score = 0.3
    combined_confidence = (0.6 * name_similarity_score + 0.4 * country_match_strictness) if name_similarity_score > 0.7 and country_match_strictness >= 0.9 else \
                          ((0.4 * name_similarity_score + 0.6 * country_match_strictness) if country_match_strictness >= 0.9 else \
                          (0.05 if country_match_strictness < 0 else (0.7 * name_similarity_score + 0.3 * location_similarity_score)))
    if original_queried_name.lower() in text_block_for_verification.lower(): combined_confidence = min(1.0, combined_confidence + 0.10)
    current_threshold = ASIAN_COMPANY_THRESHOLD if country_name_queried and is_asian_company(country_name_queried) else SIMILARITY_THRESHOLD
    return combined_confidence >= current_threshold, combined_confidence

def get_company_info(company_name, country_name):
    print_section(f"RESEARCHING (Gemini {GEMINI_MODEL_NAME}): '{company_name}' ({country_name})")
    safe_company_name_query, _ = sanitize_company_name(company_name)
    system_prompt_for_research = "You are an expert business researcher and analyst. Your primary goal is to provide accurate, structured information about specific business entities based on their role as a user or purchaser within a given industry."

    user_prompt_for_research = (
        f"The company '{safe_company_name_query}' (Original queried name: '{company_name}') is known to be a user or purchaser of {TARGET_INDUSTRY_DESCRIPTION}.\n"
        f"Your task is to identify and provide detailed information for **this specific business entity** that is headquartered or has its principal operations related to this activity in **{country_name}**.\n\n"
        "CRITICAL FOCUS:\n"
        "1.  **Identify the Correct Client Entity:** Prioritize the legal entity that matches '{safe_company_name_query}' and is the one that was/is the user/purchaser of components related to "
        f"**{TARGET_INDUSTRY_DESCRIPTION}** within **{country_name}**. Distinguish it from companies with similar names but in unrelated sectors.\n"
        "2.  **Country Specificity:** The primary operations and headquarters of the entity you detail MUST be in **{country_name}**.\n"
        "3.  **Current Operational Status:** If the original client entity was acquired or merged, identify the current operational successor entity in **{country_name}** that would have inherited the relevant role. Clearly note this transition in 'Disambiguation Notes'.\n\n"
        "INSTRUCTIONS FOR OUTPUT:\n"
        "A. IF YOU FIND THE SPECIFIC TARGET CLIENT ENTITY (or its direct successor) IN **{country_name}**:\n"
        "   Present its information in a section starting with '--- ENTITY START ---'.\n"
        "   Provide details using these field labels ON SEPARATE LINES:\n"
        "       Official Company Name: [Full Legal or commonly used English Name for THIS client entity in **{country_name}**]\n"
        "       Original Script Name: [Name in original language for THIS client entity in **{country_name}**, if applicable. Else 'N/A'.]\n"
        "       Headquarters Location: [City, **{country_name}** for THIS client entity.]\n"
        "       Website: [Official website for THIS client entity.]\n"
        "       Primary Industry/Sector: [Main industry of THIS client entity, especially as it relates to its operations.]\n"
        f"      Detailed Business Activities & Model: [Describe what THIS client entity makes or does in **{country_name}**, focusing on aspects that would involve the **{TARGET_INDUSTRY_DESCRIPTION}**.]\n"
        "       Key Products/Services (Categorized): [Main offerings of THIS client entity.]\n"
        f"      Known or Inferred Relevance to Target Industry: [REQUIRED: Based on your research, confirm or strongly infer HOW this specific entity is relevant to **{TARGET_INDUSTRY_DESCRIPTION}**. For example: 'Uses these components for machine control...' or 'Integrates these components into the equipment they build.']\n"
        "       Technology Focus / Key Specializations: [Key technologies relevant to THIS client entity's operations.]\n"
        "       Target Customer Segments: [Main customer types for THIS client entity.]\n"
        "       Disambiguation Notes: [Clarify this is the identified client entity (or its successor) in **{country_name}**. Note any name variations from the query if the core entity is correct. If it's a successor, detail the acquisition/merger if known.]\n\n"
        "B. IF MULTIPLE DISTINCT ENTITIES IN **{country_name}** ARE EXTREMELY STRONG CANDIDATES FOR BEING THE SPECIFIC CLIENT:\n"
        "   Provide separate '--- ENTITY START ---' sections for each, clearly detailing why each is a plausible candidate matching the query name and industry relevance.\n\n"
        "C. IF, AFTER THOROUGH SEARCHING, THE SPECIFIC CLIENT ENTITY (or its direct successor) CANNOT BE CLEARLY IDENTIFIED OR CONFIRMED AS RELEVANT IN **{country_name}**:\n"
        "   State: 'TARGET_CLIENT_ENTITY_NOT_CLEARLY_IDENTIFIED_IN_COUNTRY: Unable to definitively identify or confirm '{safe_company_name_query}' in **{country_name}** as a relevant entity based on available information.'\n"
        "   Optionally, if you found a company with a similar name but in a clearly unrelated industry, you can briefly note it as 'OTHER_UNRELATED_ENTITY_FOUND: [Name, Location, Brief Reason for exclusion]'.\n\n"
        "Ensure your entire response adheres to one of these output structures. Do not add conversational text outside these structures."
    )

    llm_full_response = gemini_api_call(user_prompt_for_research, system_prompt_for_research)
    if VERBOSE and llm_full_response: print_info(f"\nRaw Gemini Response (first 500 chars):\n{llm_full_response[:500]}\n")
    cleaned_llm_response = clean_llm_response(llm_full_response)
    if VERBOSE and cleaned_llm_response: print_info(f"\nCleaned Gemini Response (first 500 chars for parsing):\n{cleaned_llm_response[:500]}\n")

    structured_entities_found_from_llm = []
    if not cleaned_llm_response or "TARGET_CLIENT_ENTITY_NOT_CLEARLY_IDENTIFIED_IN_COUNTRY:" in cleaned_llm_response.upper() or "NO_INFO_FOUND_IN_COUNTRY:" in cleaned_llm_response.upper():
        logger.warning(f"Gemini indicated NO_SPECIFIC_CLIENT_ENTITY or NO_INFO_FOUND for {company_name} in {country_name}.")
    else:
        entity_text_blocks = [block.strip() for block in regex.split(r"--- ENTITY START ---", cleaned_llm_response, flags=re.IGNORECASE) if block.strip()]
        if not entity_text_blocks and "Official Company Name:" in cleaned_llm_response:
            print_warning("No '--- ENTITY START ---' separators found, attempting to parse as single entity.")
            entity_text_blocks = [cleaned_llm_response]
        print_info(f"Found {len(entity_text_blocks)} potential entity blocks in Gemini response.")

        for i, block_text in enumerate(entity_text_blocks):
            if len(block_text) < MIN_DESCRIPTION_LENGTH: continue
            print_info(f"Processing Entity Block #{i+1}...")
            entity_data = {"Input Company Name Query": company_name, "Input Country Query": country_name, **{f: "Not parsed" for f in PHASE_1_OUTPUT_FIELDS if f not in ["Input Company Name Query", "Input Country Query"]}, "Original_LLM_Response_Block_Snippet": block_text[:5000] + "..." if len(block_text) > 3000 else block_text}
            
            parsing_labels_map = {
                "Official Company Name": "Entity_Official_Company_Name", 
                "Original Script Name": "Original Script Name", 
                "Headquarters Location": "Entity_Headquarters_Location", 
                "Website": "Entity_Website", 
                "Primary Industry/Sector": "Entity_Primary_Industry_Sector", 
                "Detailed Business Activities & Model": "Entity_Detailed_Business_Activities_Model", 
                "Key Products/Services (Categorized)": "Entity_Key_Products_Services_Categorized", 
                "Known or Inferred Relevance to Target Industry": "Industry_Relevance_Details",
                "Technology Focus / Key Specializations": "Entity_Technology_Focus_Specializations", 
                "Target Customer Segments": "Entity_Target_Customer_Segments", 
                "Disambiguation Notes": "Entity_Disambiguation_Notes"
            }
            
            for label_in_prompt, dict_key_in_entity_data in parsing_labels_map.items():
                if not is_asian_company(country_name) and label_in_prompt == "Original Script Name": continue
                other_labels_escaped = [regex.escape(l_prompt) for l_prompt in parsing_labels_map.keys() if l_prompt != label_in_prompt]
                lookahead = r"(?=\n\s*(?:" + "|".join(other_labels_escaped) + r"):\s*|$)" if other_labels_escaped else r"(?=$)"
                match = regex.search(rf"^\s*{regex.escape(label_in_prompt)}:\s*(.*?){lookahead}", block_text, regex.MULTILINE | regex.DOTALL | regex.IGNORECASE)
                if match:
                    value = match.group(1).strip().replace("[Specify]", "").replace("[Provide detail or 'Not specified']", "").strip()
                    entity_data[dict_key_in_entity_data] = value if value and value.lower() not in ["n/a", "-"] else "Not specified"
            
            text_for_verification = f"Official Company Name: {entity_data.get('Entity_Official_Company_Name', '')}\nHeadquarters Location: {entity_data.get('Entity_Headquarters_Location', '')}"
            is_block_match, block_confidence = verify_company_match(text_for_verification, company_name, country_name)
            entity_data["Entity_Block_Match_Confidence"] = f"{block_confidence:.2f}"
            entity_data["Entity_Block_Is_Likely_Match"] = "Yes" if is_block_match else "No"
            
            if entity_data.get("Entity_Official_Company_Name", "Not parsed").lower() not in ["not parsed", "not specified", "target_client_entity_not_clearly_identified_in_country"]:
                structured_entities_found_from_llm.append(entity_data)
                print_success(f"Parsed potential entity: '{entity_data['Entity_Official_Company_Name']}' with conf {block_confidence:.2f}")
            else:
                print_warning(f"Skipped block #{i+1}: '{entity_data.get('Entity_Official_Company_Name', 'N/A')}' (Not parsed or LLM negative).")

    final_selected_entity_list = []
    if structured_entities_found_from_llm:
        candidate_entities = []
        for entity_dict in structured_entities_found_from_llm:
            if "TARGET_CLIENT_ENTITY_NOT_CLEARLY_IDENTIFIED_IN_COUNTRY" in entity_dict.get("Entity_Official_Company_Name", "") or "OTHER_UNRELATED_ENTITY_FOUND" in entity_dict.get("Entity_Disambiguation_Notes", ""):
                print_warning(f"Post-filter: LLM indicated '{entity_dict.get('Entity_Official_Company_Name')}' not target/irrelevant.")
                continue
            if country_name.lower() not in entity_dict.get("Entity_Headquarters_Location", "").lower():
                print_warning(f"Post-filter: Skipping '{entity_dict.get('Entity_Official_Company_Name')}' (loc mismatch).")
                continue
            
            relevance_text = entity_dict.get("Industry_Relevance_Details", "n/a").lower()
            relevance_score = 0.5 if len(relevance_text) > 10 and "not applicable" not in relevance_text and "n/a" not in relevance_text else 0.0
            if any(kw in relevance_text for kw in RELEVANCE_KEYWORDS):
                relevance_score = 1.0
            
            name_loc_conf = float(entity_dict.get("Entity_Block_Match_Confidence", "0.0"))
            notes = entity_dict.get("Entity_Disambiguation_Notes", "").lower()
            activity_score = 1.0
            if "successor to" in notes or ("acquired by" in notes and "current operational entity" in notes):
                activity_score = 0.9
            elif any(s in notes for s in ["acquired by", "no longer exists", "merged into", "defunct"]):
                activity_score = 0.2
                
            final_score = (name_loc_conf * 0.3) + (relevance_score * 0.5) + (activity_score * 0.2)
            
            if "TARGET_CLIENT_ENTITY_NOT_CLEARLY_IDENTIFIED" in entity_dict.get("Entity_Official_Company_Name",""):
                final_score = 0
            
            if final_score > MINIMUM_ACCEPTABLE_SCORE_FOR_CLIENT:
                candidate_entities.append({"entity_data": entity_dict, "final_score": final_score})
            else:
                print_info(f"Post-filter: Entity '{entity_dict.get('Entity_Official_Company_Name')}' rejected. Score: {final_score:.2f}")
            
        if candidate_entities:
            best_candidate_data = sorted(candidate_entities, key=lambda x: x["final_score"], reverse=True)[0]['entity_data']
            print_success(f"Selected best client entity in {country_name}: '{best_candidate_data['Entity_Official_Company_Name']}' (Score: {sorted(candidate_entities, key=lambda x: x['final_score'], reverse=True)[0]['final_score']:.2f})")
            final_selected_entity_list = [best_candidate_data]
            
    if not final_selected_entity_list:
        logger.warning(f"No suitable client entity profile identified for '{company_name}' in '{country_name}'.")
        return [{"Input Company Name Query": company_name, "Input Country Query": country_name, "Entity_Official_Company_Name": "No specific client entity profile identified", "Industry_Relevance_Details": "N/A", **{field: "Processing failed to identify target client" for field in PHASE_1_OUTPUT_FIELDS if field not in ["Input Company Name Query", "Input Country Query", "Entity_Official_Company_Name", "Industry_Relevance_Details"]}, "Original_LLM_Response_Block_Snippet": cleaned_llm_response[:5000] if cleaned_llm_response else "No response from LLM.", "Entity_Block_Match_Confidence": "0.00", "Entity_Block_Is_Likely_Match": "No"}]
        
    return final_selected_entity_list

def save_results(results_list_for_excel, output_file_path, columns_to_write):
    try:
        if not results_list_for_excel: print_warning("No results to save."); return True
        df = pd.DataFrame(results_list_for_excel)
        for col_name in columns_to_write:
            if col_name not in df.columns: df[col_name] = "N/A_Col_Missing_In_Data"
        df = df[columns_to_write]
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
        row_for_excel = {col_name_excel: entity_data_dict.get(col_name_excel, "Data N/A") for col_name_excel in PHASE_1_OUTPUT_FIELDS}
        all_results_accumulator_list.append(row_for_excel)
        if entity_data_dict.get("Entity_Official_Company_Name") != "No specific client entity profile identified":
            print_success(f"Appended entity '{entity_data_dict.get('Entity_Official_Company_Name', 'Unknown Entity')}' for query '{company_name_query}'.")
        logger.info(f"Processed entity '{entity_data_dict.get('Entity_Official_Company_Name', 'Unknown Entity')}' for query '{company_name_query}'. Confidence: {entity_data_dict.get('Entity_Block_Match_Confidence')}")
    if not list_of_found_entities: # Should not happen with current get_company_info logic
        print_warning(f"No entities (even placeholder) from get_company_info for query '{company_name_query}'. Adding placeholder.")
        placeholder = {field: f"Critical Failure for {company_name_query}" for field in PHASE_1_OUTPUT_FIELDS}
        placeholder.update({"Input Company Name Query": company_name_query, "Input Country Query": country_name_query, "Known or Inferred HMI/IPC Relevance": "N/A"})
        all_results_accumulator_list.append(placeholder)
    save_results(all_results_accumulator_list, OUTPUT_FILE, PHASE_1_OUTPUT_FIELDS)

# --- MODIFIED process_companies_from_csv ---
def process_companies_from_csv(csv_file_path_param):
    # This print_section uses standard print() and will appear ABOVE the progress bar
    print_section(f"STARTING COMPANY PROCESSING - GEMINI ({GEMINI_MODEL_NAME})")

    try:
        if not os.path.exists(csv_file_path_param):
            msg = f"CRITICAL: Input CSV file not found at {csv_file_path_param}"; print_error(msg); logger.critical(msg); return []
        df_input = pd.read_csv(csv_file_path_param, encoding='utf-8')
        if 'country' not in df_input.columns or 'company_name' not in df_input.columns:
            msg = "Input CSV must have 'company_name' and 'country' columns."; print_error(msg); logger.error(msg); return []
        print_success(f"Successfully loaded CSV with {len(df_input)} queries: {csv_file_path_param}")
    except UnicodeDecodeError:
        try:
            print_warning("UTF-8 decoding failed, trying latin1...")
            df_input = pd.read_csv(csv_file_path_param, encoding='latin1')
            if 'country' not in df_input.columns or 'company_name' not in df_input.columns:
                 msg = "Input CSV (latin1) must have 'company_name' and 'country' columns."; print_error(msg); logger.error(msg); return []
            print_success(f"Successfully loaded CSV with {len(df_input)} queries (latin1): {csv_file_path_param}")
        except Exception as e_latin1: print_error(f"Error reading CSV file {csv_file_path_param} with latin1: {e_latin1}"); return []
    except Exception as e: print_error(f"Error reading CSV file {csv_file_path_param}: {e}"); return []

    all_results_for_excel = []
    processed_input_queries_set = set()

    if os.path.exists(OUTPUT_FILE):
        try:
            print_info(f"Found existing output file: {OUTPUT_FILE}. Loading processed queries.")
            existing_df = pd.read_excel(OUTPUT_FILE)
            for col in PHASE_1_OUTPUT_FIELDS: # Ensure all expected columns exist for appending
                if col not in existing_df.columns: existing_df[col] = "N/A_Prior_Run"
            all_results_for_excel.extend(existing_df.to_dict('records'))
            if "Input Company Name Query" in existing_df.columns and "Input Country Query" in existing_df.columns:
                for _, row in existing_df.iterrows():
                    processed_input_queries_set.add((str(row["Input Company Name Query"]), str(row["Input Country Query"])))
            print_success(f"Loaded {len(all_results_for_excel)} existing records. Found {len(processed_input_queries_set)} processed queries.")
        except Exception as e:
            print_warning(f"Could not load existing results from {OUTPUT_FILE}: {e}.")
            all_results_for_excel = [] # Start fresh if loading fails
            processed_input_queries_set = set()
    else:
        print_info(f"No existing output file found at {OUTPUT_FILE}. Starting fresh.")

    print_info(f"Output Excel will be: {OUTPUT_FILE}")

    queries_to_run_list = []
    for index, input_row in df_input.iterrows():
        company_name_val = str(input_row.get('company_name', '')).strip()
        country_name_val = str(input_row.get('country', '')).strip()
        if not company_name_val or not country_name_val:
            print_warning(f"Skipping CSV row {index+2} due to missing company name or country.")
            continue
        if (company_name_val, country_name_val) not in processed_input_queries_set:
            queries_to_run_list.append((company_name_val, country_name_val))

    if not queries_to_run_list:
        print_info("All company queries from input CSV appear processed. Nothing new to run.")
        if all_results_for_excel: # Save loaded results if any, ensuring all columns
             save_results(all_results_for_excel, OUTPUT_FILE, PHASE_1_OUTPUT_FIELDS)
        return all_results_for_excel

    print_info(f"Found {len(queries_to_run_list)} new company queries to process.")

    # Initialize the single, overall progress bar HERE.
    # It will print once. Subsequent standard print() calls will scroll below it.
    overall_progress_bar = tqdm(
        total=len(queries_to_run_list),
        desc=f"Overall Query Progress", # Initial description
        unit="query",
        position=0,  # Try to keep it at the top-most line of the terminal/output cell
        leave=True   # Keep the progress bar visible after the loop finishes
    )

    for i, (company_name_to_query, country_name_to_query) in enumerate(queries_to_run_list):
        # Update the description of the static bar to show current item
        overall_progress_bar.set_description(f"Processing: {company_name_to_query[:25]}...")

        # process_company and its sub-functions will use standard print() for console output
        process_company(company_name_to_query, country_name_to_query, all_results_for_excel)

        processed_input_queries_set.add((company_name_to_query, country_name_to_query))

        # Manually update the overall progress bar by 1 step
        overall_progress_bar.update(1)

        if i < len(queries_to_run_list) - 1:
            # This print_info uses standard print() and will scroll below the bar
            print_info(f"Waiting {DELAY} seconds before next query...")
            time.sleep(DELAY)

    overall_progress_bar.set_description("All queries processed") # Final update to bar description
    overall_progress_bar.close() # Close the progress bar
    return all_results_for_excel

def main():
    # print_section uses standard print
    print_section(f"COMPANY RESEARCH - CLIENT PROFILE (GEMINI {GEMINI_MODEL_NAME})")
    logger.info(f"Script started. Model: {GEMINI_MODEL_NAME}. Output: {OUTPUT_FILE}.")

    if not API_KEY_SET_SUCCESSFULLY and not os.getenv("GOOGLE_API_KEY"):
        print_error("CRITICAL: API Key was not set. Exiting.")
        logger.critical("CRITICAL: API Key was not set. Exiting.")
        return
    if not gemini_client_instance:
        print_error("CRITICAL: Gemini client could not be initialized. Exiting.")
        logger.critical("CRITICAL: Gemini client could not be initialized. Exiting.")
        return
    if not os.path.exists(CSV_FILE_PATH):
         msg = f"CRITICAL: Input CSV file not found: {CSV_FILE_PATH}"; print_error(msg); logger.critical(msg); return

    results = process_companies_from_csv(CSV_FILE_PATH)

    print_section(f"GEMINI ({GEMINI_MODEL_NAME}) PROCESSING COMPLETE")
    if results:
        final_df_to_assess = pd.DataFrame(results)
        num_entities_found = len(final_df_to_assess[final_df_to_assess["Entity_Official_Company_Name"] != "No specific client entity profile identified"])
        num_failed_to_identify = len(final_df_to_assess[final_df_to_assess["Entity_Official_Company_Name"] == "No specific client entity profile identified"])
        num_input_queries_processed = len(set(zip(final_df_to_assess["Input Company Name Query"], final_df_to_assess["Input Country Query"])))

        print_success(f"Research completed. Processed {num_input_queries_processed} unique input queries.")
        print_success(f"Successfully identified {num_entities_found} client entities.")
        if num_failed_to_identify > 0:
            print_warning(f"Failed to identify a specific client entity for {num_failed_to_identify} queries.")
        print_info(f"Final results saved to {OUTPUT_FILE}")
        logger.info(f"Research completed. Identified {num_entities_found} entities, failed for {num_failed_to_identify} from {num_input_queries_processed} queries. Saved to {OUTPUT_FILE}")
    elif os.path.exists(OUTPUT_FILE):
        print_info(f"No new queries were processed. Existing results are in {OUTPUT_FILE}")
        logger.info("No new queries processed. Existing results in output file.")
    else:
        print_warning("Research completed, but no results were generated and no output file exists.")
        logger.warning("Research completed, but no results were generated.")

if __name__ == "__main__":

    main()
