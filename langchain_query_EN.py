import streamlit as st
#from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from data_processing import get_duckdb_connection
import logging
import re
import os
from dotenv import load_dotenv, find_dotenv
import time
import math
import csv
import uuid

# Try to import tiktoken for accurate token counting
try:
    import tiktoken
    tiktoken_available = True
except ImportError:
    tiktoken_available = False

_ = load_dotenv(find_dotenv())

# --- STANDARD LOGGING SETUP ---
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'llm_query_app.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)

if not getattr(logger, 'has_run_before', False):
    logger.info(f"Standard logs will be saved to {log_file}")
    logger.has_run_before = True

# --- PERFORMANCE CSV LOGGING SETUP (CORRECTED) ---
PERFORMANCE_LOG_FILE = os.path.join(log_dir, 'llm_performance_log.csv')
CSV_HEADER = [
    'request_id',
    'user_question',
    'prompt_1_text',
    'prompt_1_tokens',
    'llm_1_response_sql',
    'sql_execution_time_ms',
    'prompt_2_text',
    'prompt_2_tokens',
    'llm_2_response_time_ms',
    'llm_2_final_response'
]

def log_performance_to_csv(data):
    """Appends a new row to the performance CSV log file."""
    file_exists = os.path.isfile(PERFORMANCE_LOG_FILE)
    try:
        with open(PERFORMANCE_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            if not file_exists:
                writer.writerow(CSV_HEADER)
            
            row_data = [data.get(header, '') for header in CSV_HEADER]
            writer.writerow(row_data)
    except Exception as e:
        logger.error(f"Failed to write to performance log CSV: {str(e)}")

def estimate_tokens(text):
    """Estimates the number of tokens in a given text."""
    if not isinstance(text, str):
        return 0
    if tiktoken_available:
        try:
            encoding = tiktoken.get_encoding("cl100k_base")
            return len(encoding.encode(text))
        except Exception:
            pass
    words = len(text.split())
    chars = len(text)
    return math.ceil((chars / 4 + words / 0.75) / 2)

#LLM with Groq
#llm = ChatGroq(
#    model="meta-llama/llama-4-maverick-17b-128e-instruct",
#    api_key=os.getenv('GROQ_API_KEY'),
#    temperature=0.0,
#    max_tokens=4096
#)

#LLM with OpenAI
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    api_key=os.getenv('OPENAI_API_KEY'),
    temperature=0.0,
    max_tokens=4096
)

# Prompts remain the same...
sql_prompt_template = PromptTemplate.from_template(
    """
    Instructions:
    - You are a FinOps expert, focused on answering questions about public cloud billing (AWS, Azure, GCP, Oracle Cloud).
    - Your goal is to understand the user's query and convert it into valid DuckDB SQL queries, using ONLY the `consolidated_billing` table.
    - Generate ONLY the SQL query, without explanations or comments.
    - For cost-related questions, ALWAYS include `SUM(BilledCost) AS total_cost` in the SELECT statement.
    - For questions with "top consumers" or "top application" or "main" or "top whatever", sort by `total_cost` in descending order (`ORDER BY total_cost DESC`) and use `LIMIT {top_k}` if not specified the limit in the question itself (e.g.'top 2' or 'top four' and so on).
    - For questions about the "Top consuming Provider" or "top consuming service" or "top consuming category", always in the singular, show the single Top 1 value being asked.
    - For simple total cost questions (e.g., 'consumption in January'), use only `SUM(BilledCost) AS total_cost` without a GROUP BY.
    - For questions by service (e.g., 'cost of S3'), filter by `ServiceName` and group by `ServiceName`.
    - For questions by provider (e.g., 'total Azure consumption'), filter by `ProviderName` and use `SUM(BilledCost) AS total_cost`.
    - For questions by category (e.g., 'cost of Compute'), filter by `ServiceCategory` and group by `ServiceCategory`.
    - For questions by tags (e.g., 'cost for application EvolveVaultCentral'), filter by `tag_application`, `tag_environment`, or `tag_business_unit` and group by the corresponding tag.
    - For questions combining tags and other dimensions (e.g., 'which applications consumed the most compute services'), group by `tag_application`, filter by `ServiceCategory`, and order by cost.
    - For resource analysis (e.g., 'which instances'), filter by `ResourceType` or `ServiceName` and group by `ResourceId`, including `ResourceName` if available.
    - For cost per unit (e.g., 'cost per GB'), use `SUM(BilledCost) / SUM(ConsumedQuantity)` if `ConsumedQuantity` and `ConsumedUnit` are available.
    - For comparisons or trends (e.g., 'month-over-month comparison'), use `date_trunc('month', BillingPeriodStart)` and group by the period, covering the entire dataset range.
    - If the period is not specified and the context does not indicate a month, do not add a date filter (use the entire dataset).
    - If a month is specified without a year (e.g., 'December'), use the most recent year available in the dataset, based on the context: {context}.
    - For sequential questions (e.g., 'and in February'), reuse the context: {context}.
    - Never use `SELECT *`. Choose relevant columns.
    - Never perform data modifications (`INSERT`, `UPDATE`, `DELETE`).
    - If `ConsumedQuantity` or `ConsumedUnit` are mentioned but not applicable, ignore them.

    Schema of the consolidated_billing table:
    - BilledCost: decimal (billed cost)
    - BillingPeriodStart: date (start of the billing period)
    - BillingPeriodEnd: date (end of the billing period)
    - ProviderName: string (provider name, e.g., 'AWS', 'Microsoft', 'Google Cloud', 'Oracle')
    - RegionId: string (region ID, e.g., 'us-east-1', 'global', or empty)
    - ResourceName: string (resource name, can be NULL)
    - ResourceType: string (resource type, e.g., 'Instance', 'NAT Gateway')
    - ResourceId: string (resource ID, e.g., 'arn:aws:ec2:...')
    - ServiceCategory: string (service category, e.g., 'Compute', 'Storage')
    - ServiceName: string (service name, e.g., 'Amazon Elastic Compute Cloud')
    - SubAccountName: string (sub-account name, can be NULL)
    - ConsumedQuantity: decimal (consumed quantity, e.g., GB, hours, can be NULL)
    - ConsumedUnit: string (unit of measurement, e.g., 'GB', 'Hours', can be NULL)
    - tag_application: string (application name, e.g., 'EvolveVaultCentral', can be NULL)
    - tag_environment: string (environment, e.g., 'dev', 'prod', can be NULL)
    - tag_business_unit: string (business unit, e.g., 'ChicagoIT', can be NULL)

    Examples:
    - Question: "How much did we spend on S3 in us-east-1?"
      SELECT ServiceName, SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE ServiceName = 'Amazon Simple Storage Service' AND RegionId = 'us-east-1' GROUP BY ServiceName LIMIT 5;
    - Question: "What was the cost of EC2 and EKS?"
      SELECT ServiceName, SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE ServiceName IN ('Amazon Elastic Compute Cloud', 'Amazon Elastic Kubernetes Services') GROUP BY ServiceName LIMIT 5;
    - Question: "Which services spent the most in the United States last month?"
      SELECT ServiceName, SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE RegionId IN ('us-east-1', 'us-west-1', 'us-west-2') AND BillingPeriodStart >= '2024-09-01' AND BillingPeriodStart < '2024-10-01' GROUP BY ServiceName ORDER BY total_cost DESC LIMIT 5;
    - Question: "What was the consumption in January 2024?"
      SELECT SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE BillingPeriodStart >= '2024-01-01' AND BillingPeriodStart < '2024-02-01';
    - Question: "What is the month-over-month consumption comparison?"
      SELECT date_trunc('month', BillingPeriodStart) AS month, SUM(BilledCost) AS total_cost FROM consolidated_billing GROUP BY date_trunc('month', BillingPeriodStart) ORDER BY month;
    - Question: "What is the total cloud consumption?"
      SELECT SUM(BilledCost) AS total_cost FROM consolidated_billing;
    - Question: "Which applications consumed the most compute services?"
      SELECT tag_application, SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE ServiceCategory = 'Compute' GROUP BY tag_application ORDER BY total_cost DESC LIMIT 5;
    - Question: "Cost by application and environment"
      SELECT tag_application, tag_environment, SUM(BilledCost) AS total_cost FROM consolidated_billing GROUP BY tag_application, tag_environment ORDER BY total_cost DESC LIMIT 5;
    - Question: "Cost of compute in the AWS provider?"
      SELECT ServiceCategory, SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE ServiceCategory = 'Compute' AND ProviderName = 'AWS' GROUP BY ServiceCategory LIMIT 5;
    - Question: "What is the total consumption of Azure?"
      SELECT SUM(BilledCost) AS total_cost FROM consolidated_billing WHERE ProviderName = 'Microsoft';

    Question: {question}
    Table Info: {table_info}
    Top K: {top_k}
    Context: {context}
    Generate the corresponding SQL query.
    """
)
response_prompt_template = PromptTemplate.from_template(
    """
    You are a FinOps expert, helping to interpret cloud cost data. Based on the user's question, the SQL query results, and the context, generate a short, objective, natural language response. Use the format "A: [answer]". Include only the cost value, service, region, tags, provider, or category (if applicable), and period (if specified). Avoid recommendations or additional details. If the result is empty, state that there is no data.

    Formatting Rules:
    - For questions with multiple services (e.g., 'cost of EC2 and EKS'), report: "A: [Service1]: $XXX; [Service2]: $YYY."
    - For questions without a specified period and with a specific service/category/provider (e.g., 'consumption of EC2'), use: "The total consumption of [service/category/provider] is $XXX from [start month] to [end month] as per the database."
    - For questions without a service, category, or period (e.g., 'cloud consumption'), use: "The total cloud consumption from [start month] to [end month] is $XXX."
    - For questions with a month but no year (e.g., 'consumption in December'), use the most recent year from the dataset and state: "In [month] of [year], the cost of [service/category/provider] was $XXX."
    - For questions with an explicit period, state: "In [period], the cost of [service/category/provider] was $XXX."
    - For questions with tags (e.g., 'cost of application EvolveVaultCentral'), state: "In [period], the cost of the application [tag_application] was $XXX."
    - For ranking questions (e.g., 'which applications consumed the most'), list the results: "A: [item1]: $XXX; [item2]: $YYY; ..."
    - For grouping by multiple dimensions (e.g., 'cost by application and environment'), report: "[tag_application]/[tag_environment]: $XXX."
    - For comparisons or trends, list the costs by month: "A: [month/year]: $XXX; [month/year]: $YYY; ..."

    Question: {question}
    SQL Result: {sql_result}
    Context: {context}

    Example:
    - Question: "How much did we spend on EC2 and EKS?"
      SQL Result: [('Amazon Elastic Compute Cloud', 88.28), ('Amazon Elastic Kubernetes Services', 50.00)]
      Context: {{'period_start': '2024-06-01', 'period_end': '2024-07-01'}}
      Answer: A: Amazon Elastic Compute Cloud: $88.28; Amazon Elastic Kubernetes Services: $50.00.
    - Question: "Which services spent the most in the United States last month?"
      SQL Result: [('Amazon Elastic Compute Cloud', 76.90), ('Amazon Relational Database Service', 9.77)]
      Context: {{'period_start': '2024-09-01', 'period_end': '2024-10-01'}}
      Answer: A: Amazon Elastic Compute Cloud: $76.90; Amazon Relational Database Service: $9.77 in September 2024.
    - Question: "What was the consumption in January 2024?"
      SQL Result: [(1234.56,)]
      Context: {{'period_start': '2024-01-01', 'period_end': '2024-02-01'}}
      Answer: A: In January 2024, the total cost was $1,234.56.

    Answer:
    """
)

# Included helper functions
def get_dataset_date_range():
    con = get_duckdb_connection()
    try:
        query = "SELECT MIN(BillingPeriodStart)::TIMESTAMP, MAX(BillingPeriodStart)::TIMESTAMP FROM consolidated_billing"
        return con.execute(query).fetchone()
    except Exception as e:
        logger.error(f"Error getting date range: {str(e)}")
        return None, None
    finally:
        con.close()

def get_last_year_for_month(month_num):
    con = get_duckdb_connection()
    try:
        query = f"SELECT MAX(strftime(BillingPeriodStart::TIMESTAMP, '%Y')) FROM consolidated_billing WHERE strftime(BillingPeriodStart::TIMESTAMP, '%m') = '{month_num}'"
        return con.execute(query).fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting last year for month {month_num}: {str(e)}")
        return None
    finally:
        con.close()

def preprocess_question(question):
    question = question.lower()
    month_mapping = {"january": "01", "february": "02", "march": "03", "april": "04", "may": "05", "june": "06", "july": "07", "august": "08", "september": "09", "october": "10", "november": "11", "december": "12"}
    term_mapping = { "ec2": "ServiceName = 'Amazon Elastic Compute Cloud'", "s3": "ServiceName = 'Amazon Simple Storage Service'", "rds": "ServiceName = 'Amazon Relational Database Service'", "vm": "ServiceName = 'Microsoft Azure Virtual Machines'", "compute": "ServiceCategory = 'Compute'", "storage": "ServiceCategory = 'Storage'", "networking": "ServiceCategory = 'Networking'", "database": "ServiceCategory = 'Database'", "aws": "ProviderName = 'AWS'", "azure": "ProviderName = 'Microsoft'", "gcp": "ProviderName = 'Google Cloud'", "consumption": "cost", "top": "top consumers"}
    # Simplified term mapping based on previous fixes
    if 'question_context' not in st.session_state: st.session_state.question_context = {}
    st.session_state.question_context.update({'service': None, 'category': None, 'region': None, 'provider': None, 'year': None, 'group_by': None, 'type': None, 'periods': None, 'analysis': None, 'period_start': None, 'period_end': None, 'tag_application': None, 'tag_environment': None, 'tag_business_unit': None})
    min_date, max_date = get_dataset_date_range()
    if min_date and max_date:
        st.session_state.question_context['period_start'], st.session_state.question_context['period_end'] = min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d')
    has_explicit_year = re.search(r'\d{4}', question)
    for month_name, month_num in month_mapping.items():
        if month_name in question and not has_explicit_year:
            last_year = get_last_year_for_month(month_num)
            if last_year:
                next_month = str(int(month_num) + 1).zfill(2) if int(month_num) < 12 else "01"
                next_year = last_year if int(month_num) < 12 else str(int(last_year) + 1)
                question = question.replace(month_name, f"BillingPeriodStart >= '{last_year}-{month_num}-01' AND BillingPeriodStart < '{next_year}-{next_month}-01'")
    for term, replacement in term_mapping.items(): question = question.replace(term, replacement)
    logger.info(f"Preprocessed question: {question}")
    return question

def validate_query(sql_query):
    if "select" in sql_query.lower() and "from consolidated_billing" not in sql_query.lower():
        logger.error("Invalid query: does not use the consolidated_billing table")
        return False
    return True

# --- CORRECTED FUNCTIONS ---

def generate_sql(question, table_info, top_k=3):
    processed_question = preprocess_question(question)
    chain = sql_prompt_template | llm
    context = st.session_state.get('question_context', {})
    filtered_context = {k: v for k, v in context.items() if v is not None}
    prompt_input = {"question": processed_question, "table_info": table_info, "top_k": top_k, "context": str(filtered_context)}
    prompt_text = sql_prompt_template.format(**prompt_input)
    token_count = estimate_tokens(prompt_text)
    try:
        sql_query_response = chain.invoke(prompt_input).content
        sql_query = sql_query_response.strip("```sql").strip()
        if not validate_query(sql_query):
            return None, "A: Invalid query generated.", prompt_text, token_count
        return sql_query, None, prompt_text, token_count
    except Exception as e:
        logger.error(f"Error generating SQL: {str(e)}")
        return None, f"A: Error processing question: {str(e)}", prompt_text, token_count

def execute_query(sql_query):
    if not sql_query: return None, 0
    con = get_duckdb_connection()
    try:
        start_time = time.time()
        result = con.execute(sql_query).fetchall()
        execution_time_ms = (time.time() - start_time) * 1000
        return result, execution_time_ms
    except Exception as e:
        logger.error(f"Error executing query: '{sql_query}'. Error: {str(e)}")
        return f"A: Error executing query: {str(e)}", 0
    finally:
        con.close()

def enhance_response(question, sql_result, context):
    chain = response_prompt_template | llm
    filtered_context = {k: v for k, v in context.items() if v is not None}
    prompt_input = {"question": question, "sql_result": str(sql_result), "context": str(filtered_context)}
    prompt_text = response_prompt_template.format(**prompt_input)
    token_count = estimate_tokens(prompt_text)
    try:
        start_time = time.time()
        response = chain.invoke(prompt_input).content
        response_time_ms = (time.time() - start_time) * 1000
        return response, response_time_ms, token_count, prompt_text
    except Exception as e:
        logger.error(f"Error enhancing response: {str(e)}")
        return "A: Could not format response.", 0, token_count, prompt_text

def format_response(question, result):
    if isinstance(result, str):
        return result, 0, 0, ""
    context = st.session_state.get('question_context', {})
    return enhance_response(question, result, context)

def process_question(question, table_info="Table: consolidated_billing"):
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] USER QUESTION RECEIVED: \"{question}\"")
    perf_data = {'request_id': request_id, 'user_question': question}
    try:
        sql_query, error, p1_text, p1_tokens = generate_sql(question, table_info)
        perf_data.update({'prompt_1_text': p1_text, 'prompt_1_tokens': p1_tokens, 'llm_1_response_sql': sql_query if not error else error})
        if error:
            perf_data['llm_2_final_response'] = error
            log_performance_to_csv(perf_data)
            return sql_query, error

        result, sql_time_ms = execute_query(sql_query)
        perf_data['sql_execution_time_ms'] = f"{sql_time_ms:.0f}"
        if isinstance(result, str):
            error_msg = result
            perf_data['llm_2_final_response'] = error_msg
            log_performance_to_csv(perf_data)
            return sql_query, error_msg
        
        final_response, llm2_time_ms, p2_tokens, p2_text = format_response(question, result)
        perf_data.update({'prompt_2_text': p2_text, 'prompt_2_tokens': p2_tokens, 'llm_2_response_time_ms': f"{llm2_time_ms:.0f}", 'llm_2_final_response': final_response})
        
        logger.info(f"[{request_id}] Final formatted response: \"{final_response}\"")
        log_performance_to_csv(perf_data)
        
        return sql_query, final_response
    except Exception as e:
        logger.critical(f"[{request_id}] Unexpected error in main flow: {str(e)}")
        error_msg = f"A: A critical error occurred: {str(e)}"
        perf_data['llm_2_final_response'] = error_msg
        log_performance_to_csv(perf_data)
        return None, error_msg