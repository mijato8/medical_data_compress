import logging
from bs4 import BeautifulSoup
import pandas as pd
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from crawl_last_page import get_last_page
import time
import os

# start_page = 1001
# last_page =  2000
max_workers = int(os.cpu_count() * 2)

def setup_logging():
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.INFO)

    # 파일 핸들러
    file_handler = logging.FileHandler('crawl_pront_log.log')
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
        medicines = soup.find('div', class_='r_sec_md')
        medicines2 = medicines.find_all('tr')
        medicines2.pop(0)

        n_result = []
        s_result = []
        t_result = []

        for medicine_list in medicines2:
            number_01 = medicine_list.find('td', class_="al_c")
            number = number_01.string.strip()
            n_result.append(number)

        for data_02 in medicines2:
            data_02_01 = data_02.find_all('span', recursive=True)
            for info2 in range(0, 49):
                data_04 = data_02_01[info2].text.strip()
                s_result.append(data_04)
        
        if len(s_result) > 730 or page_number == get_last_page():
            if len(s_result) < 750:
                count = -1
                for p in range(0, len(s_result) + 1):
                    count += 1
                    if s_result[count] == '품목분류' and s_result[count + 1] == '전문의약품' and s_result[count + 2] != '전문의약품':
                        s_result.insert(count + 1, "")
                    if s_result[count] == 'ATC코드' and s_result[count + 1] == '제품명':
                        s_result.insert(count + 1, "")
                    if s_result[count] == '전문의약품' and s_result[count + 1] == '완제/원료구분':
                        s_result.insert(count + 1, "")
                s_result.append("")

            for r in range(1, len(s_result), 2):
                data_05 = s_result[r]
                t_result.append(data_05)

            pront_data = []
            for w in range(1, len(n_result) + 1):
                f_result = t_result[(w - 1) * 25: 25 * w]
                f_result.insert(0, n_result[w - 1])
                f_result.insert(0, page_number)
                pront_data.append(f_result)

            return pront_data, None

        else:
            logging.warning(f"Error on page {page_number}: Data length mismatch")
            return None, page_number

    except Exception as e:
        logging.error(f"Error on page {page_number}: {e}")
        
        return None, page_number

def fetch_all_pages(start_page, last_page):
    pront_data = []
    error_pages = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_page, page_number): page_number for page_number in range(start_page, last_page + 1)}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Crawling pront_data"):
            page_data, error_page = future.result()
            if page_data:
                pront_data.extend(page_data)
            if error_page:
                error_pages.append(error_page)

    return pront_data, error_pages

def retry_error_pages(error_pages):
    final_data = []
    while error_pages:
        remaining_errors = []
        for page_number in error_pages:
            page_data, error_page = fetch_page(page_number)
            if page_data:
                final_data.extend(page_data)
            if error_page:
                remaining_errors.append(error_page)
        error_pages = remaining_errors
        if error_pages:
            logging.info(f"Retrying {len(error_pages)} remaining error pages...")

    return final_data

def remove_duplicates(df):
    duplicates = df[df.duplicated(subset=['품목기준코드'], keep=False)]
    pages_with_duplicates = duplicates['page_number'].unique()
    
    if len(pages_with_duplicates) > 0:
        logging.info(f"Found {len(pages_with_duplicates)} pages with duplicates. Retrying...")
        logging.info(f"pages_with_duplicates {pages_with_duplicates} ")
        updated_data = []
        for page_number in pages_with_duplicates:
            page_data, _ = fetch_page(page_number)
            if page_data:
                updated_data.extend(page_data)

        df_updated = pd.DataFrame(updated_data, columns=df.columns)
        df_cleaned = df[~df['page_number'].isin(pages_with_duplicates)]
        df = pd.concat([df_cleaned, df_updated], ignore_index=True)

    return df

def crawl_pront_data_to_csv(start_page,last_page):
    
    setup_logging()
    
    start_time = time.time()
    
    if last_page <= get_last_page():
        
        pront_data, error_pages = fetch_all_pages(start_page, last_page)

        if error_pages:
            logging.info(f"Retrying {len(error_pages)} error pages...")
            retry_data = retry_error_pages(error_pages)
            pront_data.extend(retry_data)

        df = pd.DataFrame(pront_data, columns=[
            'page_number', '순번', '제품명', '제품영문명', '업체명', '업체영문명', '품목기준코드', '허가번호',
            '허가일', '품목구분', '취소/취하', '취소/취하일자', '주성분', '주성분영문명', '첨가제',
            '묶음의약품보기', 'e약은요보기', '품목분류', '전문의약품', '완제/원료구분', '허가/신고', '제조/수입',
            '수입제조국', '마약구분', '신약구분', '표준코드', 'ATC코드'
        ])

        df = remove_duplicates(df)
        df.to_csv('pront_data.csv', index=False)
        
        logging.info(f'Total pront data fetched: {len(pront_data)}')
        logging.info('Data saved to pront_data.csv')
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")
        
    else:
        logging.info(f"No page {last_page}. Try again")
    
        
if __name__ == '__main__':
    crawl_pront_data_to_csv()

########################################################################################################################################################
