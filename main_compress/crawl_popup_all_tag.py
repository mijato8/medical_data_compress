# 스레드
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import json
# import time
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from tqdm import tqdm

# # 설정 로그 레벨
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # 세션 생성
# session = requests.Session()

# def fetch_html(product_code):
#     try:
#         base_url = 'https://nedrug.mfds.go.kr/pbp/CCBBB01/getItemDetail?itemSeq='
#         url = f'{base_url}{product_code}'
#         response = session.get(url, timeout=60) 
#         response.raise_for_status()
#         return response.text  # response.text
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Error while fetching product code {product_code}: {e}")
#         return None

# def parse_table(product_code):
#     try:
#         html_content = fetch_html(product_code)
#         if html_content:
#             soup = BeautifulSoup(html_content, 'lxml')
#             table = soup  # .find('section', class_=['drug_content', 'drug_list'])
#             if table:
#                 return (product_code, str(table))  # 튜플 형태로 반환
#             else:
#                 logging.warning(f"No data found in the table for product code {product_code}")
#                 return (product_code, None)
#         else:
#             logging.warning(f"Failed to fetch HTML content for product code {product_code}")
#             return (product_code, None)
#     except Exception as e:
#         logging.error(f"Error while parsing product code {product_code}: {e}")
#         return (product_code, None)

# def worker(product_code):
#     result = parse_table(product_code)
#     if result[1] is not None:
#         return result
#     else:
#         return (product_code, None)

# def retry_crawl_popup_pages(error_pages, num_threads):
#     final_popup_data = []
#     while error_pages:
#         new_error_list = []

#         with ThreadPoolExecutor(max_workers=num_threads) as executor:
#             futures = {executor.submit(worker, product_code): product_code for product_code in error_pages}
#             progress_bar = tqdm(total=len(futures), desc="Retrying Product Code Crawling", position=1)
            
#             for future in as_completed(futures):
#                 result = future.result()
#                 if result[1] is not None:
#                     final_popup_data.append(result)
#                 else:
#                     new_error_list.append(result[0])
#                 progress_bar.update(1)
            
#             progress_bar.close()
#         error_pages = new_error_list

#     return final_popup_data

# def crawl_popup_data_all_tag_to_json():
#     start_time = time.time()
#     num_threads = 10  # 사용할 스레드 수 설정

#     product_codes_df = pd.read_csv('product_codes.csv')
#     product_codes = product_codes_df['product_code'].tolist()

#     results = []
#     error_pages = []
    
#     with ThreadPoolExecutor(max_workers=num_threads) as executor:
#         futures = {executor.submit(worker, product_code): product_code for product_code in product_codes}
#         progress_bar = tqdm(total=len(futures), desc="main_popup_all_tag Crawling", position=0)
        
#         for future in as_completed(futures):
#             result = future.result()
#             if result[1] is not None:
#                 results.append(result)
#             else:
#                 error_pages.append(result[0])
#             progress_bar.update(1)
        
#         progress_bar.close()

#     logging.info(f"Errors occurred on pages: {error_pages}")

#     if error_pages:
#         retry_results = retry_crawl_popup_pages(error_pages, num_threads)
#         results.extend(retry_results)

#     results_dict = [{'product_code': product_code, 'tag': table} for product_code, table in results]
#     logging.info(f"Total Tag: {len(results_dict)}")

#     with open('all_codes_output.json', 'w', encoding='utf-8') as f:
#         json.dump(results_dict, f, ensure_ascii=False, indent=4)
#         logging.info("All HTML codes saved successfully in JSON format.")

#     end_time = time.time()
#     elapsed_time = end_time - start_time

#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)
#     logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

# if __name__ == "__main__":
#     crawl_popup_data_all_tag_to_json()

########################################################################################################################################
# 프로세싱
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import json
# import time
# import logging
# from concurrent.futures import ProcessPoolExecutor, as_completed
# from tqdm import tqdm

# # 설정 로그 레벨
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # 세션 생성
# session = requests.Session()

# def fetch_html(product_code):
#     try:
#         base_url = 'https://nedrug.mfds.go.kr/pbp/CCBBB01/getItemDetail?itemSeq='
#         url = f'{base_url}{product_code}'
#         response = session.get(url, timeout=60)
#         response.raise_for_status()
#         return response.text  # response.text
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Error while fetching product code {product_code}: {e}")
#         return None

# def parse_table(product_code):
#     try:
#         html_content = fetch_html(product_code)
#         if html_content:
#             soup = BeautifulSoup(html_content, 'lxml')
#             table = soup  # .find('section', class_=['drug_content', 'drug_list'])
#             if table:
#                 return (product_code, str(table))  # 튜플 형태로 반환
#             else:
#                 logging.warning(f"No data found in the table for product code {product_code}")
#                 return (product_code, None)
#         else:
#             logging.warning(f"Failed to fetch HTML content for product code {product_code}")
#             return (product_code, None)
#     except Exception as e:
#         logging.error(f"Error while parsing product code {product_code}: {e}")
#         return (product_code, None)

# def worker(product_code):
#     result = parse_table(product_code)
#     if result[1] is not None:
#         return result
#     else:
#         return (product_code, None)

# def retry_crawl_popup_pages(error_pages, num_processes):
#     final_popup_data = []
#     while error_pages:
#         new_error_list = []

#         with ProcessPoolExecutor(max_workers=num_processes) as executor:
#             futures = {executor.submit(worker, product_code): product_code for product_code in error_pages}
#             progress_bar = tqdm(total=len(futures), desc="Retrying Product Code Crawling", position=1)

#             for future in as_completed(futures):
#                 result = future.result()
#                 if result[1] is not None:
#                     final_popup_data.append(result)
#                 else:
#                     new_error_list.append(result[0])
#                 progress_bar.update(1)

#             progress_bar.close()
#         error_pages = new_error_list

#     return final_popup_data

# def crawl_popup_data_all_tag_to_json():
#     start_time = time.time()
#     num_processes = 8  # 사용할 프로세스 수 설정 (CPU 코어 수의 1~2배가 일반적)

#     product_codes_df = pd.read_csv('product_codes.csv')
#     product_codes = product_codes_df['product_code'].tolist()

#     results = []
#     error_pages = []

#     with ProcessPoolExecutor(max_workers=num_processes) as executor:
#         futures = {executor.submit(worker, product_code): product_code for product_code in product_codes}
#         progress_bar = tqdm(total=len(futures), desc="main_popup_all_tag Crawling", position=0)

#         for future in as_completed(futures):
#             result = future.result()
#             if result[1] is not None:
#                 results.append(result)
#             else:
#                 error_pages.append(result[0])
#             progress_bar.update(1)

#         progress_bar.close()

#     logging.info(f"Errors occurred on pages: {error_pages}")

#     if error_pages:
#         retry_results = retry_crawl_popup_pages(error_pages, num_processes)
#         results.extend(retry_results)

#     results_dict = [{'product_code': product_code, 'tag': table} for product_code, table in results]
#     logging.info(f"Total Tag: {len(results_dict)}")

#     with open('all_codes_output.json', 'w', encoding='utf-8') as f:
#         json.dump(results_dict, f, ensure_ascii=False, indent=4)
#         logging.info("All HTML codes saved successfully in JSON format.")

#     end_time = time.time()
#     elapsed_time = end_time - start_time

#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)
#     logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

# if __name__ == "__main__":
#     crawl_popup_data_all_tag_to_json()
########################################################################################################################################
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import os

def setup_logging():
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)

    # 파일 핸들러
    file_handler = logging.FileHandler('crawl_popup_all_tag_log.log')
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 로그 포맷 설정
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)



# 세션 생성
session = requests.Session()

def fetch_html(product_code):
    try:
        base_url = 'https://nedrug.mfds.go.kr/pbp/CCBBB01/getItemDetail?itemSeq='
        url = f'{base_url}{product_code}'
        response = session.get(url, timeout=60)
        response.raise_for_status()
        return response.text  # response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error while fetching product code {product_code}: {e}")
        return None

def parse_table(product_code):
    try:
        html_content = fetch_html(product_code)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup  # .find('section', class_=['drug_content', 'drug_list'])
            if table.find('div' , class_ = 'drug_container') is not None: 
                return (product_code, str(table))  # 튜플 형태로 반환
            else:
                logging.warning(f"No data found in the table for product code {product_code}")
                return (product_code, None)
        else:
            logging.warning(f"Failed to fetch HTML content for product code {product_code}")
            return (product_code, None)
    except Exception as e:
        logging.error(f"Error while parsing product code {product_code}: {e}")
        return (product_code, None)

def worker(product_code):
    result = parse_table(product_code)
    if result[1] is not None:
        return result
    else:
        return (product_code, None)

def retry_crawl_popup_pages(error_pages, num_processes):
    final_popup_data = []
    while error_pages:
        new_error_list = []

        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            futures = {executor.submit(worker, product_code): product_code for product_code in error_pages}
            progress_bar = tqdm(total=len(futures), desc="Retrying Product Code Crawling", position=1)

            for future in as_completed(futures):
                result = future.result()
                if result[1] is not None:
                    final_popup_data.append(result)
                else:
                    new_error_list.append(result[0])
                progress_bar.update(1)

            progress_bar.close()
        error_pages = new_error_list

    return final_popup_data

def crawl_popup_data_all_tag_to_json():
    start_time = time.time()
    
    setup_logging()
    
    num_processes = int(os.cpu_count() * 2)

    product_codes_df = pd.read_csv('product_codes.csv')
    product_codes = product_codes_df['product_code'].tolist()

    results = []
    error_pages = []

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = {executor.submit(worker, product_code): product_code for product_code in product_codes}
        progress_bar = tqdm(total=len(futures), desc="main_popup_all_tag Crawling", position=0)

        for future in as_completed(futures):
            result = future.result()
            if result[1] is not None:
                results.append(result)
            else:
                error_pages.append(result[0])
            progress_bar.update(1)

        progress_bar.close()

    logging.info(f"Errors occurred on pages: {error_pages}")

    if error_pages:
        retry_results = retry_crawl_popup_pages(error_pages, num_processes)
        results.extend(retry_results)

    results_dict = [{'product_code': product_code, 'tag': table} for product_code, table in results]
    logging.info(f"Total Tag: {len(results_dict)}")

    with open('all_codes_output.json', 'w', encoding='utf-8') as f:
        json.dump(results_dict, f, ensure_ascii=False, indent=4)
        logging.info("All HTML codes saved successfully in JSON format.")

    end_time = time.time()
    elapsed_time = end_time - start_time

    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")


if __name__ == "__main__":
    crawl_popup_data_all_tag_to_json()
