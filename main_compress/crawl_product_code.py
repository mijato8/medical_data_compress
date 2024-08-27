import logging
import os
import pandas as pd
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup
from crawl_last_page import get_last_page
from tqdm import tqdm
import time

# start_page = 1001
# last_page = 2000
max_workers = int(os.cpu_count() * 2)

def setup_logging():
    logger = logging.getLogger()
    if logger.hasHandlers():
        # 기존 핸들러를 제거
        logger.handlers.clear()

    logger.setLevel(logging.INFO)

    # 파일 핸들러
    file_handler = logging.FileHandler('crawl_product_code_log.log')
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

def fetch_page(page_number):
    try:
        url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
        response = requests.get(url, timeout=60)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='dr_table2 dr_table_type2')
        tbody = table.find('tbody')
        spans = tbody.find_all('span')

        page_codes = []
        if len(spans) > 730 or page_number == get_last_page():
            for j, i in enumerate(spans):
                if '품목기준코드' in i.text:
                    if j + 1 < len(spans):
                        next_value = spans[j + 1].text
                        page_codes.append(next_value)
            return [{'page_number': page_number, 'product_code': code} for code in page_codes], None
        else:
            logging.warning(f'Page {page_number} has an error')
            return None, page_number
    except Exception as e:
        logging.error(f'Error on page {page_number}: {e}')
        return None, page_number

def fetch_all_pages(start_page, last_page):
    product_codes = []
    error_pages = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_page, page_number): page_number for page_number in range(start_page, last_page + 1)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Crawl product_code"):
            page_codes, error_page = future.result()
            if page_codes:
                product_codes.extend(page_codes)
            if error_page:
                error_pages.append(error_page)

    return product_codes, error_pages

def retry_error_pages(error_pages):
    final_codes = []
    while error_pages:  
        remaining_errors = []
        for page_number in error_pages:
            page_codes, error_page = fetch_page(page_number)
            if page_codes:
                final_codes.extend(page_codes)
            if error_page:
                remaining_errors.append(error_page)
        error_pages = remaining_errors 

        if error_pages:
            logging.info(f"Retrying {len(error_pages)} remaining error pages...")

    return final_codes

def update_duplicates_info(pages_with_duplicates):
    updated_codes = []
    for page_number in tqdm(pages_with_duplicates, desc="Updating pages"):
        page_codes, _ = fetch_page(page_number)
        if page_codes:
            updated_codes.extend(page_codes)
    return updated_codes

def crawl_product_code_to_csv(start_page,last_page):
    start_time = time.time()
    
    setup_logging() 
    
    if last_page <= get_last_page():
        
        product_codes, error_pages = fetch_all_pages(start_page, last_page)

        if error_pages:
            logging.info(f"Retrying {len(error_pages)} error pages...")
            retry_codes = retry_error_pages(error_pages)
            product_codes.extend(retry_codes)
            
        logging.info(f'Total product codes fetched: {len(product_codes)}')
        logging.info(f'Failed pages: {len(error_pages)}')

        df = pd.DataFrame(product_codes)

        while True:
            duplicates = df[df.duplicated(subset=['product_code'], keep=False)]
            pages_with_duplicates = duplicates['page_number'].unique()
            
            if len(pages_with_duplicates) == 0:
                logging.info("No duplicates found.")
                break

            logging.info(f"Found {len(pages_with_duplicates)} pages with duplicates. Retrying...")
            logging.info(f"pages_with_duplicates {pages_with_duplicates} ")
            updated_codes = update_duplicates_info(pages_with_duplicates)
            
            # 업데이트된 데이터를 기존 데이터와 결합
            df_updated = pd.DataFrame(updated_codes)
            df_cleaned = df[~df['page_number'].isin(pages_with_duplicates)]
            df = pd.concat([df_cleaned, df_updated], ignore_index=True)

        # 최종 데이터 저장
        df.to_csv('product_codes.csv', index=False)
        logging.info('Final product codes saved to product_codes.csv')
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        
        logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

    else:
        logging.info(f"No page {last_page}. Try again")
        
if __name__ == '__main__':
    
    crawl_product_code_to_csv()
