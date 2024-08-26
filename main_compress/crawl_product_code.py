# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import time
# import logging
# from queue import Queue
# from threading import Thread, Lock
# from tqdm import tqdm
# from crwal_last_page import get_last_page

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# url = 'https://nedrug.mfds.go.kr/searchDrug'

# start_page = 1001
# last_page = 2000 #get_last_page(url)
# num_threads = 10  # 사용할 스레드 수 설정

# queue_lock = Lock()  # 큐 접근을 위한 락
# results_lock = Lock()  # 결과 접근을 위한 락

# def crawl_page(page_number):
#     try:
#         url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
#         response = requests.get(url)
#         response.raise_for_status()  # HTTP 에러 발생 시 예외 처리
#         soup = BeautifulSoup(response.text, 'html.parser')
#         number = []
#         table = soup.find('table', class_='dr_table2 dr_table_type2')
#         tbody = table.find('tbody')
#         td = tbody.find_all('span')
#         for i in td:
#             number.append(i.text)
        
#         num = []
#         tr = tbody.find_all('tr')
#         for j, i in enumerate(tr):
#             al_c = i.find('td', class_='al_c').text
#             num.append(al_c)
                
#         product_code = []

#         for i, j in enumerate(number):
#             if j == '품목기준코드':
#                 product_code.append(number[i + 1])
        
#         return product_code
    
#     except Exception as e:
#         logging.error(f"Error while parsing page {page_number}: {e}")
#         return page_number

# def worker(queue, results, error_pages, progress_bar):
#     while not queue.empty():
#         with queue_lock:
#             if queue.empty():
#                 break
#             page_number = queue.get()
        
#         result = crawl_page(page_number)
#         if isinstance(result, list):
#             with results_lock:
#                 results.extend(result)
#         else:
#             with results_lock:
#                 error_pages.append(result)
        
#         queue.task_done()
#         progress_bar.update(1)

# def retry_crawl_pages(error_pages):
#     final_product_codes = []
#     while error_pages:
#         queue = Queue()
#         for page_number in error_pages:
#             queue.put(page_number)
        
#         results = []
#         new_error_list = []
#         threads = []

#         progress_bar = tqdm(total=queue.qsize(), desc="Retrying Product Code Crawling", position=1)
        
#         for _ in range(num_threads):
#             t = Thread(target=worker, args=(queue, results, new_error_list, progress_bar))
#             t.start()
#             threads.append(t)
        
#         for t in threads:
#             t.join()
        
#         progress_bar.close()
        
#         final_product_codes.extend(results)
#         error_pages = new_error_list
        
#     return final_product_codes

# def crawl_product_code_to_csv():
#     start_time = time.time()  # 시작 시간 기록

#     queue = Queue()
#     for page_number in range(start_page, last_page + 1):
#         queue.put(page_number)
    
#     results = []
#     error_pages = []
#     threads = []

#     progress_bar = tqdm(total=queue.qsize(), desc="Product Code Crawling", position=0)
    
#     for _ in range(num_threads):
#         t = Thread(target=worker, args=(queue, results, error_pages, progress_bar))
#         t.start()
#         threads.append(t)
    
#     for t in threads:
#         t.join()
    
#     progress_bar.close()
    
#     logging.info(f"Errors occurred on pages: {error_pages}")
    
#     if error_pages:
#         retry_results = retry_crawl_pages(error_pages)
#         results.extend(retry_results)
    
#     df = pd.DataFrame(results, columns=['product_codes'])
#     df.to_csv('product_codes.csv', index=False)
    
#     logging.info(f"Total product codes: {len(results)}")

#     end_time = time.time()
#     elapsed_time = end_time - start_time
#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)

#     logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

# if __name__ == "__main__":
#     crawl_product_code_to_csv()
###################################################################################################################################################
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import time
# import logging
# from queue import Queue
# from threading import Thread, Lock
# from tqdm import tqdm
# from crwal_last_page import get_last_page

# # 설정 로그 레벨
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# start_page = 1001
# last_page = 2000  # get_last_page(url)
# num_threads = 10  # 사용할 스레드 수 설정

# queue_lock = Lock()  # 큐 접근을 위한 락
# results_lock = Lock()  # 결과 접근을 위한 락

# def crawl_page(page_number):
#     try:
#         url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
#         response = requests.get(url)
#         response.raise_for_status()  # HTTP 에러 발생 시 예외 처리
#         soup = BeautifulSoup(response.text, 'html.parser')
#         number = []
#         table = soup.find('table', class_='dr_table2 dr_table_type2')
#         tbody = table.find('tbody')
#         td = tbody.find_all('span')
#         for i in td:
#             number.append(i.text)
        
#         num = []
#         tr = tbody.find_all('tr')
#         for j, i in enumerate(tr):
#             al_c = i.find('td', class_='al_c').text
#             num.append(al_c)
                
#         product_code = []

#         for i, j in enumerate(number):
#             if j == '품목기준코드':
#                 product_code.append(number[i + 1])
        
#         return product_code
    
#     except Exception as e:
#         logging.error(f"Error while parsing page {page_number}: {e}")
#         return page_number

# def worker(queue, results, error_pages, progress_bar):
#     while not queue.empty():
#         with queue_lock:
#             if queue.empty():
#                 break
#             page_number = queue.get()
        
#         result = crawl_page(page_number)
#         if isinstance(result, list):
#             with results_lock:
#                 results.extend(result)
#             # 성공적으로 처리된 페이지는 error_pages에서 제거
#             with queue_lock:
#                 if page_number in error_pages:
#                     error_pages.remove(page_number)
#         else:
#             with queue_lock:
#                 if page_number not in error_pages:
#                     error_pages.append(page_number)
        
#         queue.task_done()
#         progress_bar.update(1)

# def crawl_product_code_to_csv():
#     start_time = time.time()

#     queue = Queue()
#     for page_number in range(start_page, last_page + 1):
#         queue.put(page_number)
    
#     results = []
#     error_pages = []  
#     threads = []

#     progress_bar = tqdm(total=queue.qsize(), desc="Product Code Crawling", position=0)
    
#     for _ in range(num_threads):
#         t = Thread(target=worker, args=(queue, results, error_pages, progress_bar))
#         t.start()
#         threads.append(t)
    
#     for t in threads:
#         t.join()
    
#     progress_bar.close()

#     logging.info(f"Initial errors occurred on pages: {error_pages}")

#     while error_pages:
#         logging.info(f"Retrying pages: {error_pages}")
        
#         retry_queue = Queue()
#         for page_number in error_pages:
#             retry_queue.put(page_number)
        
#         retry_results = []
#         retry_threads = []
#         retry_progress_bar = tqdm(total=retry_queue.qsize(), desc="Retrying Product Code Crawling", position=1)
        
#         for _ in range(num_threads):
#             t = Thread(target=worker, args=(retry_queue, retry_results, error_pages, retry_progress_bar))
#             t.start()
#             retry_threads.append(t)
        
#         for t in retry_threads:
#             t.join()
        
#         retry_progress_bar.close()
        
#         results.extend(retry_results)

#     df = pd.DataFrame(results, columns=['product_codes'])
#     df.to_csv('product_codes.csv', index=False)
    
#     logging.info(f"Total product codes: {len(results)}")

#     end_time = time.time()
#     elapsed_time = end_time - start_time
#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)

#     logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

# if __name__ == "__main__":
#     crawl_product_code_to_csv()

##################################################################################################################################################
# from bs4 import BeautifulSoup
# import pandas as pd
# import requests
# from concurrent.futures import ProcessPoolExecutor, as_completed
# from crwal_last_page import get_last_page
# import os
# from tqdm import tqdm
# import time

# start_page = 4001
# last_page = 4200
# max_workers = int(os.cpu_count() * 2)

# def fetch_page(page_number):
#     try:
#         url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
#         response = requests.get(url, timeout=60)
#         soup = BeautifulSoup(response.text, 'html.parser')
#         table = soup.find('table', class_='dr_table2 dr_table_type2')
#         tbody = table.find('tbody')
#         spans = tbody.find_all('span')

#         page_codes = []
#         if len(spans) > 730 or page_number == get_last_page():
#             for j, i in enumerate(spans):
#                 if '품목기준코드' in i.text:
#                     if j + 1 < len(spans):
#                         next_value = spans[j + 1].text
#                         page_codes.append(next_value)
#             # 각 코드마다 딕셔너리 형태로 반환
#             return [{'page_number': page_number, 'product_code': code} for code in page_codes], None
#         else:
#             print(f'Page {page_number} has an error')
#             return None, page_number
#     except Exception as e:
#         print(f'Error on page {page_number}: {e}')
#         return None, page_number

# def fetch_all_pages(start_page, last_page):
#     product_codes = []
#     error_pages = []

#     with ProcessPoolExecutor(max_workers=max_workers) as executor:
#         futures = {executor.submit(fetch_page, page_number): page_number for page_number in range(start_page, last_page + 1)}

#         for future in tqdm(as_completed(futures), total=len(futures), desc="Crawl product_code"):
#             page_codes, error_page = future.result()
#             if page_codes:
#                 product_codes.extend(page_codes)
#             if error_page:
#                 error_pages.append(error_page)

#     return product_codes, error_pages

# def retry_error_pages(error_pages):
#     final_codes = []
#     while error_pages:  
#         remaining_errors = []
#         for page_number in error_pages:
#             page_codes, error_page = fetch_page(page_number)
#             if page_codes:
#                 final_codes.extend(page_codes)
#             if error_page:
#                 remaining_errors.append(error_page)
#         error_pages = remaining_errors 

#         if error_pages:
#             print(f"Retrying {len(error_pages)} remaining error pages...")

#     return final_codes

# def update_duplicates_info(pages_with_duplicates):
#     # 중복된 페이지 정보를 재크롤링하여 업데이트하는 함수
#     updated_codes = []
#     for page_number in tqdm(pages_with_duplicates, desc="Updating pages"):
#         page_codes, _ = fetch_page(page_number)
#         if page_codes:
#             updated_codes.extend(page_codes)
#     return updated_codes

# def crawl_product_code_to_csv():    
#     product_codes, error_pages = fetch_all_pages(start_page, last_page)

#     if error_pages:
#         print(f"Retrying {len(error_pages)} error pages...")
#         retry_codes = retry_error_pages(error_pages)
#         product_codes.extend(retry_codes)
        
#     print(f'Total product codes: {len(product_codes)}')
#     print(f'Failed pages: {len(error_pages)}')

#     # 결과 저장
#     df = pd.DataFrame(product_codes)
#     df.to_csv('product_codes.csv', index=False)

#     # 중복, 누락 확인
#     while True:
#         df = pd.read_csv('product_codes.csv')
        
#         # 중복된 product_code가 포함된 페이지 식별
#         duplicates = df[df.duplicated(subset=['product_code'], keep=False)]
#         pages_with_duplicates = duplicates['page_number'].unique()
#         print('duplicates pages' , pages_with_duplicates)
        
#         if len(pages_with_duplicates) == 0:
#             print("No duplicates found.")
#             break

#         print(f"Found {len(pages_with_duplicates)} pages with duplicates. Retrying...")
#         updated_codes = update_duplicates_info(pages_with_duplicates)
        
#         # 업데이트된 데이터를 기존 데이터와 결합
#         df_updated = pd.DataFrame(updated_codes)
#         df_cleaned = df[~df['page_number'].isin(pages_with_duplicates)]
#         df_final = pd.concat([df_cleaned, df_updated], ignore_index=True)
        
#         # 최종 데이터 저장
#         df_final.to_csv('product_codes.csv', index=False)

# if __name__ == '__main__':
#     start_time = time.time()
    
#     crawl_product_code_to_csv()

#     end_time = time.time()
#     elapsed_time = end_time - start_time
#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)

#     print(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

##################################################################################################################################################
# from bs4 import BeautifulSoup
# import pandas as pd
# import requests
# from concurrent.futures import ProcessPoolExecutor, as_completed
# from crwal_last_page import get_last_page
# import os
# from tqdm import tqdm
# import time
# import logging

# start_page = 4001
# last_page = 4100
# max_workers = int(os.cpu_count() * 2)

# def fetch_page(page_number):
#     try:
#         url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
#         response = requests.get(url, timeout=60)
#         soup = BeautifulSoup(response.text, 'html.parser')
#         table = soup.find('table', class_='dr_table2 dr_table_type2')
#         tbody = table.find('tbody')
#         spans = tbody.find_all('span')

#         page_codes = []
#         if len(spans) > 730 or page_number == get_last_page():
#             for j, i in enumerate(spans):
#                 if '품목기준코드' in i.text:
#                     if j + 1 < len(spans):
#                         next_value = spans[j + 1].text
#                         page_codes.append(next_value)
#             return [{'page_number': page_number, 'product_code': code} for code in page_codes], None
#         else:
#             logging.warning(f'Page {page_number} has an error')
#             return None, page_number
#     except Exception as e:
#         logging.error(f'Error on page {page_number}: {e}')
#         return None, page_number

# def fetch_all_pages(start_page, last_page):
#     product_codes = []
#     error_pages = []

#     with ProcessPoolExecutor(max_workers=max_workers) as executor:
#         futures = {executor.submit(fetch_page, page_number): page_number for page_number in range(start_page, last_page + 1)}

#         for future in tqdm(as_completed(futures), total=len(futures), desc="Crawl product_code"):
#             page_codes, error_page = future.result()
#             if page_codes:
#                 product_codes.extend(page_codes)
#             if error_page:
#                 error_pages.append(error_page)

#     return product_codes, error_pages

# def retry_error_pages(error_pages):
#     final_codes = []
#     while error_pages:  
#         remaining_errors = []
#         for page_number in error_pages:
#             page_codes, error_page = fetch_page(page_number)
#             if page_codes:
#                 final_codes.extend(page_codes)
#             if error_page:
#                 remaining_errors.append(error_page)
#         error_pages = remaining_errors 

#         if error_pages:
#             logging.info(f"Retrying {len(error_pages)} remaining error pages...")

#     return final_codes

# def update_duplicates_info(pages_with_duplicates):
#     updated_codes = []
#     for page_number in tqdm(pages_with_duplicates, desc="Updating pages"):
#         page_codes, _ = fetch_page(page_number)
#         if page_codes:
#             updated_codes.extend(page_codes)
#     return updated_codes

# def crawl_product_code_to_csv():
#     start_time = time.time()
     
#     # logging.basicConfig(filename='crawl_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
       
#     product_codes, error_pages = fetch_all_pages(start_page, last_page)

#     if error_pages:
#         logging.info(f"Retrying {len(error_pages)} error pages...")
#         retry_codes = retry_error_pages(error_pages)
#         product_codes.extend(retry_codes)
        
#     logging.info(f'Total product codes fetched: {len(product_codes)}')
#     logging.info(f'Failed pages: {len(error_pages)}')

#     df = pd.DataFrame(product_codes)

#     while True:
#         duplicates = df[df.duplicated(subset=['product_code'], keep=False)]
#         pages_with_duplicates = duplicates['page_number'].unique()
        
#         if len(pages_with_duplicates) == 0:
#             logging.info("No duplicates found.")
#             break

#         logging.info(f"Found {len(pages_with_duplicates)} pages with duplicates. Retrying...")
#         updated_codes = update_duplicates_info(pages_with_duplicates)
        
#         # 업데이트된 데이터를 기존 데이터와 결합
#         df_updated = pd.DataFrame(updated_codes)
#         df_cleaned = df[~df['page_number'].isin(pages_with_duplicates)]
#         df = pd.concat([df_cleaned, df_updated], ignore_index=True)

#     # 최종 데이터 저장
#     df.to_csv('product_codes.csv', index=False)
#     logging.info('Final product codes saved to product_codes.csv')

#     end_time = time.time()
#     elapsed_time = end_time - start_time
#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)
    
#     logging.info(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

# if __name__ == '__main__':
        
#     crawl_product_code_to_csv()

# ##############################################################################################################################
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
