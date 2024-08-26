import pymongo
import json
import zlib
from pymongo import MongoClient
from bs4 import BeautifulSoup
import re
import csv
import requests



##########################################################################################################
# def decompress_data(compressed_data):
#     """Decompress data using zlib."""
#     return json.loads(zlib.decompress(compressed_data).decode('utf-8'))

# def load_compressed_data(product_code):
#     """Load and decompress data from MongoDB."""
#     client = pymongo.MongoClient('mongodb://localhost:27017')
#     compressed_db = client.medical_data_compressed
#     collection = compressed_db.all_tag_data_col
#     # parsing_data = client.parsing_data_compressed

    
#     document = collection.find_one({'product_code':201903522}) #201801366  #201110421
#     if document and 'data' in document:
#         compressed_data = document['data']
#         return decompress_data(compressed_data)
#     else:
#         raise ValueError(f"No data found for product_code '{product_code}'")


# document = load_compressed_data(201903522) #201801366 #201110421 #201903522
# print(document['product_code'])

# soup = BeautifulSoup(document['tag'], 'html.parser')
# soup_05 = BeautifulSoup(document['tag'], 'html.parser')
# # print(soup)
# mid_tag_name = re.findall(r'<div class="[^"]*scroll[^"]*" id="[^"]*">', str(soup))
# if '<div class="info_sec _preview notPkInfo scroll_05" id="scroll_05">' in mid_tag_name:
#     mid_scroll_05_tag = soup_05.find('div' , class_='info_sec _preview notPkInfo scroll_05')
#     for table in mid_scroll_05_tag.find_all('table', class_='indent'):
#         table.decompose()
#     # print(mid_scroll_05_tag)
#     mid_scroll_05_info = mid_scroll_05_tag.find('div', class_ = 'info_box mt30 pt0 notice').text          # mid_scroll_05 사용상의주의사항
#     print(mid_scroll_05_info)
#     print('//////////')
# if '<div class="info_sec _preview notPkInfo scroll_05" id="scroll_05">' in mid_tag_name:
#     mid_scroll_05_tag_01 = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
#     if mid_scroll_05_tag_01.find_all('table', class_='indent'):
#         mid_scroll_05_indent = mid_scroll_05_tag_01.find_all('table', class_='indent')
#         mid_scroll_05_table = [str(k) for k in mid_scroll_05_indent]
#         print(mid_scroll_05_table)
#     mid_scroll_05_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
    
    # if mid_scroll_05_tag.find_all('div' , class_= '_table_wrap_out'):
    #     table_05 = mid_scroll_05_tag.find_all('div' , class_= '_table_wrap_out')
    #     for i in table_05:
    #         rows = i.find_all('tr')

    #     # Extract and print headers
    #         headers = []
    #         for th in rows[0].find_all(['th', 'td']):
    #             headers.append(th.get_text(strip=True))
    #         print("Headers:", headers)

    #         # Extract and print rows
    #         for row in rows[1:]:
    #             cells = row.find_all(['td'])
    #             data = [cell.get_text(strip=True) for cell in cells]
    #             print("Row data:", data)
#     # if mid_scroll_05_tag.find('table' , class_='indent'):
#     mid_scroll_05_table = soup.find_all('div' , class_='_table_wrap_out')
#     print(mid_scroll_05_table)
    # mid_scroll_05_info = mid_scroll_05_tag.find('div', class_ = 'info_box mt30 pt0 notice').text          # mid_scroll_05 사용상의주의사항
    # print(mid_scroll_05_info)
    # mid_scroll_05_dict = {'product_code': document['product_code'] , 'mid_scroll_05_data':mid_scroll_05_info}




##################################################################################################################################################################
# client = MongoClient('mongodb://localhost:27017')
# db = client.medical_data_chunk
# db_parsing_data = client['parsing_data']

# def get_large_document(product_code):
#     client = pymongo.MongoClient('mongodb://localhost:27017')
#     db = client.medical_data_chunk
#     collection = db.all_tag_data_col

#     chunks = collection.find({'product_code': 201107216}).sort('chunk_index')
#     document_str = ''.join(chunk['chunk_data'] for chunk in chunks)
#     return json.loads(document_str)

# # Example usage
# document = get_large_document('some_product_code')
# # print(document)
# soup = BeautifulSoup(document['tag'], 'html.parser')
# mid_tag_name = re.findall(r'<div class="[^"]*scroll[^"]*" id="[^"]*">', str(soup))
# print(mid_tag_name)
# scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
# scroll_01_div = scroll_01_tag.find('div' , class_='pc-img')
# scroll_01_img = scroll_01_div.find_all('img')
# if scroll_01_img:
#     scroll_01_img_src = [i['src'] for i in scroll_01_img]
#     scroll_01_img_src_dic = dict({'product_code':document['product_code'] , 'img': scroll_01_img_src})
#     filter = {'product_code':scroll_01_img_src_dic['product_code']}
#     update_data = {'$set':scroll_01_img_src_dic}
#     db_parsing_data.scroll_01_img.update_one(filter , update_data , upsert=True)
#     # print('product_info_scroll_01_img update to db')

################################################################################
# client = MongoClient('mongodb://localhost:27017')
# db = client.medical_data
# db_medical_data = db['pront_data_col']
# parsing_db = client.parsing_data_compressed
# compressed_medical_db = client.medical_data_compressed

# all_tag_data = compressed_medical_db['all_tag_data_col'].find()
# parsing_data = parsing_db['mid_scroll_02_col'].find()

# mid_02 = []
# for i in parsing_data:
#     mid_02.append(i['product_code'])

# all_tag = []
# for i in all_tag_data:
#     all_tag.append(i['product_code'])

# difference = [item for item in all_tag if item not in mid_02]
# print(difference)
################################################################################

# code = db_medical_data.find()
# db_code = []
# for i in code:
#     db_code.append(i['품목기준코드'])

#######################################################################################
# pront_data_path = 'pront_data.csv'
# product_codes_path = 'product_codes.csv'


# real_code = []
# with open(pront_data_path, newline='', encoding='utf-8') as csvfile:
#         csv_reader = csv.DictReader(csvfile)
#         rows = list(csv_reader)  # 모든 행을 미리 로드하여 총 행 수를 알 수 있게 함
#         total_rows = len(rows)
#         for i in rows:
#             real_code.append(i['품목기준코드'])

# duplicates = list(set([item for item in real_code if real_code.count(item) > 1]))

# print(duplicates)
# print(len(duplicates))
########################################################################################
# real_code = []
# with open(product_codes_path, newline='', encoding='utf-8') as csvfile:
#         csv_reader = csv.DictReader(csvfile)
#         rows = list(csv_reader)  # 모든 행을 미리 로드하여 총 행 수를 알 수 있게 함
#         total_rows = len(rows)
#         for i in rows:
#             real_code.append(i['product_codes'])

# duplicates = list(set([item for item in real_code if real_code.count(item) > 1]))

# print(duplicates)
# print(len(duplicates))


# session = requests.Session()
# base_url = 'https://nedrug.mfds.go.kr/pbp/CCBBB01/getItemDetail?itemSeq='
# url = f'{base_url}{200403304}'
# response = session.get(url)
# soup = BeautifulSoup(response.text, 'html.parser')
# soup01 = BeautifulSoup(response.text, 'html.parser')
# soup02 = BeautifulSoup(response.text, 'html.parser')

# mid_scroll_04_tag = soup02.find('div' , class_='info_sec _preview notPkInfo scroll_04')
# tables = mid_scroll_04_tag.find_all('table', class_='indent')
# # 각 테이블의 위치에 따라 문자열 추가
# for index, table in enumerate(tables):
#     # 테이블의 부모 요소를 가져옴
#     parent = table.parent
#     # 테이블의 위치에 맞춰 새로운 문자열 생성
#     new_text = soup02.new_string(f'table{index+1}')
#     # 테이블을 제거하고 새로운 문자열을 추가
#     table.decompose()
#     parent.insert_before(new_text)

# mid_scroll_04_info = mid_scroll_04_tag.find('div' , class_ = 'info_box mt20 pt0').text                 # mid_scroll_04 용법용량
# print(mid_scroll_04_info)
# print('///////')


# mid_scroll_05_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
# tables = mid_scroll_05_tag.find_all('table', class_='indent')
#         # 각 테이블의 위치에 따라 문자열 추가
# for index, table in enumerate(tables):
#     # 테이블의 부모 요소를 가져옴
#     parent = table.parent
    
#     # 테이블의 위치에 맞춰 새로운 문자열 생성
#     new_text = soup.new_string(f'table{index+1}')
    
#     # 테이블을 제거하고 새로운 문자열을 추가
#     table.decompose()
#     parent.insert_before(new_text)
# mid_scroll_05_info = mid_scroll_05_tag.find('div', class_ = 'info_box mt30 pt0 notice').text          # mid_scroll_05 사용상의주의사항
# print(mid_scroll_05_info)
# print('//////////////////')
# print('//////////////////')


# mid_scroll_04_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_04')
# if mid_scroll_04_tag.find_all('table', class_='indent'):
#     mid_scroll_04_indent = mid_scroll_04_tag.find_all('table', class_='indent')
#     mid_scroll_04_table = [str(k) for k in mid_scroll_04_indent]
#     print(len(mid_scroll_04_table))
#     print(mid_scroll_04_table)


# mid_scroll_05_tag = soup01.find('div' , class_='info_sec _preview notPkInfo scroll_05')
# if mid_scroll_05_tag.find_all('table', class_='indent'):
#     mid_scroll_05_indent = mid_scroll_05_tag.find_all('table', class_='indent')
#     mid_scroll_05_table = [str(i) for i in mid_scroll_05_indent]
#     print(len(mid_scroll_05_table))
#     print(mid_scroll_05_table)
#     print('/////////')





# for table in soup.find_all('table', class_='indent'):
#     table.decompose()
# mid_scroll_05_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
# mid_scroll_05_info = mid_scroll_05_tag.find('div', class_ = 'info_box mt30 pt0 notice').text          # mid_scroll_05 사용상의주의사항
# mid_scroll_05_indent = mid_scroll_05_tag.find_all('table', class_='indent')


###
# mid_scroll_05_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
# mid_scroll_05_indent = mid_scroll_05_tag.find_all('table', class_='indent')
# mid_scroll_05_table = [str(i) for i in mid_scroll_05_indent]
# mid_scroll_05_dict = {'product_code': i['product_code'] , 'mid_scroll_05_table':mid_scroll_05_table}
# filter = {'product_code':mid_scroll_05_dict['product_code']}
# update_data = {'$set':mid_scroll_05_dict}
# compressed_parsing_db.mid_scroll_05_table.update_one(filter , update_data , upsert=True)





# client = pymongo.MongoClient('mongodb://localhost:27017')
# parsing_data = client.parsing_data_compressed
# collection = parsing_data['mid_scroll_05_']

# document = collection.find_one({'product_code':201903522}) #201801366  #201110421

# mid_scroll_05_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
# tables = soup.find_all('table', class_='indent')
#         # 각 테이블의 위치에 따라 문자열 추가
# for index, table in enumerate(tables):
#     # 테이블의 부모 요소를 가져옴
#     parent = table.parent
    
#     # 테이블의 위치에 맞춰 새로운 문자열 생성
#     new_text = soup.new_string(f'table{index+1}')
    
#     # 테이블을 제거하고 새로운 문자열을 추가
#     table.decompose()
#     parent.insert_before(new_text)
# mid_scroll_05_info = mid_scroll_05_tag.find('div', class_ = 'info_box mt30 pt0 notice').text          # mid_scroll_05 사용상의주의사항
# print(mid_scroll_05_info)





# mid_scroll_05_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
# if mid_scroll_05_tag.find_all('table', class_='indent'):
#     mid_scroll_05_indent = mid_scroll_05_tag.find_all('table', class_='indent')
#     mid_scroll_05_table = [str(k) for k in mid_scroll_05_indent]
# print(document)





########################################################################################################################

# def decompress_data(compressed_data):
#     """Decompress data using zlib."""
#     return json.loads(zlib.decompress(compressed_data).decode('utf-8'))

# def load_all_compressed_data():
#     """Load and decompress all data from MongoDB."""
#     client = pymongo.MongoClient('mongodb://localhost:27017')
#     compressed_db = client.medical_data_compressed
#     collection = compressed_db.all_tag_data_col
    
#     documents = collection.find()
    
#     all_data = []
#     for document in documents:
#         if 'data' in document:
#             compressed_data = document['data']
#             decompressed_data = decompress_data(compressed_data)
#             all_data.append(decompressed_data)
    
#     return all_data

# # 모든 데이터를 로드
# all_decompressed_data = load_all_compressed_data()

# # 모든 데이터를 출력
# for data in all_decompressed_data:
#     soup = BeautifulSoup(data['tag'], 'html.parser')
#     mid_scroll_03_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_03')
#     if mid_scroll_03_tag.find_all('table', class_='indent'):
#         print(data['product_code'])








# product_code = 201207358   #202106959  #202007109
# session = requests.Session()
# base_url = 'https://nedrug.mfds.go.kr/pbp/CCBBB01/getItemDetail?itemSeq='
# url = f'{base_url}{product_code}'
# response = session.get(url)
# soup = BeautifulSoup(response.text, 'html.parser')
# mid_tag_name = re.findall(r'<div class="[^"]*scroll[^"]*" id="[^"]*">', str(soup))

# scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
# product_info = scroll_01_tag.find('table', class_ = 's-dr_table dr_table_type1')   
# if 'btn_small btn_base btn_point2' in str(product_info):
#     scroll_01_link = product_info.find_all('button' , class_='btn_small btn_base btn_point2')  #['onclick']
#     scroll_01_links_03 = []
#     for i in scroll_01_link:
#         file_id = re.search(r"'(.*?)'", i['onclick']).group(1)
#         filepath = f'https://nedrug.mfds.go.kr{file_id}'
#         scroll_01_links_03.append(filepath)
#     scroll_01_link_path_03 = {'product_code':product_code , 'scroll_01_button_path':scroll_01_links_03}
#     print(scroll_01_link_path_03)
    # filter = {'product_code':scroll_01_link_path_03['product_code']}
    # update_data = {'$set':scroll_01_link_path_03}
    # compressed_parsing_db.scroll_01_link_03.update_one(filter , update_data , upsert=True)

# if '<div class="info_sec notPkInfo scroll_02" id="scroll_02">' in mid_tag_name:
#         mid_scroll_02_tag = soup.find('div', class_='info_sec notPkInfo scroll_02', id='scroll_02')    # mid_scroll_02_tag 
#         scroll_02_href = mid_scroll_02_tag.find_all('a')
#         link=[]
#         for i in scroll_02_href:
#             filepath = f'https://nedrug.mfds.go.kr{i['href']}'
#             link.append(filepath)
#         print(link)
        # if scroll_02_href:
        #     scroll_02_link = [i['href'] for i in scroll_02_href if '상세보기' not in i]
        #     if len(scroll_02_link) != 0 :
        #         scroll_12_link_dic = {'product_code':i['product_code'] , 'scroll_02_link_01': scroll_02_link}
        #         filter = {'product_code':scroll_12_link_dic['product_code']}
        #         update_data = {'$set':scroll_12_link_dic}
        #         compressed_parsing_db.scroll_12_link_01.update_one(filter , update_data , upsert=True)




##########################################################################################
# def table_dict():
#     table_cols_tag = i.find_all('th')                         # mid table col
#     table_cols = [i.text for i in table_cols_tag]
#     table_col = []
#     table_column = []

#     for item in table_cols:
#         if item == '순번':
#             if table_column:
#                 table_col.append(table_column)
#             table_column = [item]
#         else:
#             table_column.append(item)

#     if table_column:
#         table_col.append(table_column)

#     table_values_tag = i.find_all('td')
#     table_values = [i.text for i in table_values_tag]
#     mid_col_len = len(table_col[0])
#     table_values_list = [table_values[i:i+mid_col_len] for i in range(0, len(table_values), mid_col_len)]
#     mid_table = [dict(zip(table_col[0], item)) for item in table_values_list]

#     return mid_table

# mid_scroll_02_tag = soup.find('div', class_='info_sec notPkInfo scroll_02', id='scroll_02')    # mid_scroll_02_tag 
# tags_to_extract = [
#     ('h3', "cont_title3 mt27 pb10"),
#     ('table', 'tb_base'),
#     ('h3', "cont_title4 mt3 pb10 pl10"),
#     ('h3',"cont_title5 mt0 pb13 pl10")
# ]

# result = []

# for element in mid_scroll_02_tag.descendants:  # soup 내 모든 요소를 순회
#     if isinstance(element, str):  # 텍스트 노드는 건너뜀
#         continue
#     for tag, class_name in tags_to_extract:
#         # 클래스 목록을 문자열로 결합하여 비교
#         if element.name == tag and " ".join(element.get('class', [])) == class_name:
#             result.append(element)  
#             break 

# mid_scroll02_data = []

# for i in result:
#     if i.name == 'h3' and "cont_title3 mt27 pb10" in " ".join(i.get('class', [])):
#         mid_scroll02_data.append(i.text)
#     elif i.name == 'table' and "tb_base" in " ".join(i.get('class', [])):
#         mid_scroll02_data.append(table_dict())
#     elif i.name == 'h3' and "cont_title4 mt3 pb10 pl10" in " ".join(i.get('class', [])):
#         mid_scroll02_data.append(i.text)
#     elif i.name == 'h3' and "cont_title5 mt0 pb13 pl10" in " ".join(i.get('class', [])):
#         mid_scroll02_data.append(i.text)
        
# for i in result2:
#     print(i)
##############################################################################################################












# print(mid_scroll_02_tag)
# tb_base = mid_scroll_02_tag.find('table' , class_='tb_base')

# tags = mid_scroll_02_tag.find_all('h3', class_=lambda x: x in ['cont_title3 mt27 pb10', 'cont_title4 mt3 pb10 pl10'])
# combined_tags = [tag.text for tag in tags]  # 태그를 문자열로 결합
# print(combined_tags)




# # print(len(table_cols_tag))
# # print(mid_scroll02_data)
# print(mid_info_top)
# print(mid_scroll_02_table_dict)
# print(mid_info_bottom)
# # print(mid_scroll_02_table_dict)
# # print(mid_scroll02_data)



# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.chrome.options import Options

# import time
# from bs4 import BeautifulSoup
# import requests

# start_page = 4001
# last_page = 4002

# driver = webdriver.Chrome() 
# options = Options()
# options.add_argument('--headless') 
# for page in range(start_page,last_page+1):
#     driver.get(f'https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page}')    
#     soup = BeautifulSoup(driver.page_source, 'html.parser')
#     print(soup)


# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from bs4 import BeautifulSoup
# import time

# start_page = 4935
# last_page = 4937

# def initialize_driver():
#     options = Options()
#     options.add_argument('--headless')  # 브라우저 UI 없이 실행
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
    
#     # WebDriver 초기화
#     driver = webdriver.Chrome(options=options)
#     return driver

# def main():
#     driver = initialize_driver()
    
#     product_codes = []
    
#     for page in range(start_page, last_page + 1):
#         driver.get(f'https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page}')
#         soup = BeautifulSoup(driver.page_source, 'html.parser')
#         table = soup.find('table' , class_= 'dr_table2 dr_table_type2')
#         tbody = table.find('tbody')
#         span = tbody.find_all('span')
        
#         for j, i in enumerate(span):
#             if '품목기준코드' in i.text:
#                 if j + 1 < len(span): 
#                     next_value = span[j + 1].text
#                     product_codes.append(next_value)
#     print(product_codes)
#     print(len(product_codes))
#     return product_codes
        
#     time.sleep(1)  # 페이지 요청 간 딜레이

#     driver.quit()

# if __name__ == "__main__":
#     main()

##############################################################################################################################
# # 기본코드
# from bs4 import BeautifulSoup
# import pandas as pd
# from crwal_last_page import get_last_page

# start_page = 6001
# last_page =  6050  

# product_codes = []
# error_pages = []
    
# for page_number in range(start_page, last_page + 1):    
#     try:
#         print(page_number)
#         url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
#         response = requests.get(url)
#         soup = BeautifulSoup(response.text, 'html.parser')
#         table = soup.find('table' , class_= 'dr_table2 dr_table_type2')
#         tbody = table.find('tbody')
#         spans = tbody.find_all('span')
#         print('span',len(spans))
        
#         if len(spans) > 730 or page_number == get_last_page():
#             for j, i in enumerate(spans):
#                 if '품목기준코드' in i.text:
#                     if j + 1 < len(spans): 
#                         next_value = spans[j + 1].text
#                         product_codes.append(next_value)
#                         print(j , next_value)
#         else:
#             error_pages.append(page_number)
        
#     except Exception as e:
#         error_pages.append(page_number)

# print(len(product_codes))
# print(len(error_pages))

# df = pd.DataFrame(product_codes, columns=['product_codes'])
# df.to_csv('product_codes.csv', index=False)

######################################################################################################
    
    # product_codes_path = 'product_codes.csv'
    
    # df = pd.read_csv(product_codes_path)
    # duplicates = df[df.duplicated(subset=['product_code'], keep=False)]
    # pages_with_duplicates = duplicates['page_number'].unique()    
    # df_cleaned = df[~df['page_number'].isin(pages_with_duplicates)]
    # update_duplicates_info
    
    
    # print(pages_with_duplicates)
    
    # real_code = []
    
    # with open(product_codes_path, newline='', encoding='utf-8') as csvfile:
    #     csv_reader = csv.DictReader(csvfile)
    #     rows = list(csv_reader)
    #     total_rows = len(rows)
    #     for i in rows:            
    #         real_code.append(i['product_code'])
            
    # duplicates= list(set([item for item in real_code if real_code.count(item) > 1]))

    # print(duplicates)
    # print(len(duplicates))
######################################################################################################

# from bs4 import BeautifulSoup
# import pandas as pd
# import requests
# from concurrent.futures import ProcessPoolExecutor, as_completed
# from crwal_last_page import get_last_page
# import os
# from tqdm import tqdm
# import time

# start_page = 4001
# last_page = 4500
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

#         for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching pages"):
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

# def main():    
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
    
#     main()

#     end_time = time.time()
#     elapsed_time = end_time - start_time
#     hours, rem = divmod(elapsed_time, 3600)
#     minutes, seconds = divmod(rem, 60)

#     print(f"Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")
######################################################################################################
# 기본코드
# from bs4 import BeautifulSoup
# import pandas as pd
# from crwal_last_page import get_last_page

# start_page = 6001
# last_page =  6002  

# result_data = []
# error_pages = []
    
# for page_number in range(start_page, last_page + 1):    
#     try:
#         base_url = 'https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page='
#         url = f'{base_url}{page_number}'
        
#         response = requests.get(url, timeout=10)
#         response.raise_for_status()
        
#         soup = BeautifulSoup(response.text, 'html.parser')
#         medicines = soup.find('div', class_='r_sec_md')
#         medicines2 = medicines.find_all('tr')
#         medicines2.pop(0)
        
#         n_result = []
#         s_result = []
#         t_result = []
        
#         for medicine_list in medicines2:
#             number_01 = medicine_list.find('td', class_="al_c")
#             number = number_01.string.strip()
#             n_result.append(number)
        
#         for data_02 in medicines2:
#             data_02_01 = data_02.find_all('span', recursive=True)
#             for info2 in range(0, 49):
#                 data_04 = data_02_01[info2].text.strip()
#                 s_result.append(data_04)
        
#         if len(s_result) < 750:
#             count = -1
#             for p in range(0, len(s_result) + 1):
#                 count += 1
#                 if s_result[count] == '품목분류' and s_result[count + 1] == '전문의약품' and s_result[count + 2] != '전문의약품':
#                     s_result.insert(count + 1, "")
#                 if s_result[count] == 'ATC코드' and s_result[count + 1] == '제품명':
#                     s_result.insert(count + 1, "")
#                 if s_result[count] == '전문의약품' and s_result[count + 1] == '완제/원료구분':
#                     s_result.insert(count + 1, "")
#             s_result.append("")
        
#         for r in range(1, len(s_result), 2):
#             data_05 = s_result[r]
#             t_result.append(data_05)
        
        
#         for w in range(1, len(n_result) + 1):
#             f_result = t_result[(w - 1) * 25: 25 * w]
#             f_result.insert(0, n_result[w - 1])
#             result_data.append(f_result)
        
#     except Exception as e:
#         error_pages.append(page_number)

# print(n_result)
# print(len(n_result))
# print(t_result)
# print('///')
# print(len(result_data))
# print(len(error_pages))

# head = ['순번','제품명','제품영문명','업체명','업체영문명','품목기준코드','허가번호','허가일','품목구분','취소/취하','취소/취하일자','주성분','주성분영문명','첨가제','묶음의약품보기','e약은요보기','품목분류','전문의약품','완제/원료구분','허가/신고','제조/수입','수입제조국','마약구분','신약구분','표준코드','ATC코드']

# df = pd.DataFrame(result_data, columns=head)
# df.to_csv('pront_data.csv', header=head, index=None)


######################################################################################################
# ## pront 기본코드
# from bs4 import BeautifulSoup
# import pandas as pd
# from crwal_last_page import get_last_page

# start_page = 6001
# last_page =  6002  

# pront_data = []
# error_pages = []
    
# for page_number in range(start_page, last_page + 1):    
#     try:
#         print(page_number)
#         url = f"https://nedrug.mfds.go.kr/searchDrug?sort=&sortOrder=&searchYn=&ExcelRowdata=&page={page_number}"
#         response = requests.get(url)
#         soup = BeautifulSoup(response.text, 'html.parser')
#         medicines = soup.find('div', class_='r_sec_md')
#         medicines2 = medicines.find_all('tr')
#         medicines2.pop(0)
        
#         n_result = []
#         s_result = []
#         t_result = []
        
#         for medicine_list in medicines2:
#             number_01 = medicine_list.find('td', class_="al_c")
#             number = number_01.string.strip()
#             n_result.append(number)
        
#         for data_02 in medicines2:
#             data_02_01 = data_02.find_all('span', recursive=True)
#             for info2 in range(0, 49):
#                 data_04 = data_02_01[info2].text.strip()                            
#                 s_result.append(data_04)
        
#         if len(s_result) > 730 or page_number == get_last_page():            
#             if len(s_result) < 750:
#                 count = -1
#                 for p in range(0, len(s_result) + 1):
#                     count += 1
#                     if s_result[count] == '품목분류' and s_result[count + 1] == '전문의약품' and s_result[count + 2] != '전문의약품':
#                         s_result.insert(count + 1, "")
#                     if s_result[count] == 'ATC코드' and s_result[count + 1] == '제품명':
#                         s_result.insert(count + 1, "")
#                     if s_result[count] == '전문의약품' and s_result[count + 1] == '완제/원료구분':
#                         s_result.insert(count + 1, "")
#                 s_result.append("")
            
#             for r in range(1, len(s_result), 2):
#                 data_05 = s_result[r]
#                 t_result.append(data_05)
            
#             for w in range(1, len(n_result) + 1):
#                 f_result = t_result[(w - 1) * 25: 25 * w]
#                 f_result.insert(0, n_result[w - 1])
#                 f_result.insert(0, page_number)
#                 pront_data.append(f_result)
            
#         else:
#             error_pages.append(page_number)
            
#     except Exception as e:
#         error_pages.append(page_number)

# print(len(pront_data))
# print(len(error_pages))

# head = ['page_number','순번','제품명','제품영문명','업체명','업체영문명','품목기준코드','허가번호','허가일','품목구분','취소/취하','취소/취하일자','주성분','주성분영문명','첨가제','묶음의약품보기','e약은요보기','품목분류','전문의약품','완제/원료구분','허가/신고','제조/수입','수입제조국','마약구분','신약구분','표준코드','ATC코드']

# df = pd.DataFrame(pront_data, columns=head)
# df.to_csv('pront_data.csv', header=head, index=None)
######################################################################################################
# import pandas as pd

# product_codes_path = 'product_codes.csv'
# pront_data_path = 'pront_data.csv'

# df = pd.read_csv(pront_data_path)
# duplicates = df[df.duplicated(subset=['품목기준코드'], keep=False)]
# pages_with_duplicates = duplicates['page_number'].unique()    
# df_cleaned = df[~df['page_number'].isin(pages_with_duplicates)]

# print(df_cleaned)



# real_code = []

# with open(product_codes_path, newline='', encoding='utf-8') as csvfile:
#     csv_reader = csv.DictReader(csvfile)
#     rows = list(csv_reader)
#     total_rows = len(rows)
#     for i in rows:            
#         real_code.append(i['product_code'])
        
# duplicates= list(set([item for item in real_code if real_code.count(item) > 1]))

# print(duplicates)
# print(len(duplicates))

# 201607314 , 201708388 , 201804441 , 201907969 , 202106193


# def decompress_data(compressed_data):
#     """Decompress data using zlib."""
#     return json.loads(zlib.decompress(compressed_data).decode('utf-8'))

# def load_compressed_data(product_code):
#     """Load and decompress data from MongoDB."""
#     client = pymongo.MongoClient('mongodb://localhost:27017')
#     compressed_db = client.medical_data_compressed
#     collection = compressed_db.all_tag_data_col
#     # parsing_data = client.parsing_data_compressed

    
#     document = collection.find_one({'product_code':201708388}) #201801366  #201110421
#     if document and 'data' in document:
#         compressed_data = document['data']
#         return decompress_data(compressed_data)
#     else:
#         raise ValueError(f"No data found for product_code '{product_code}'")


# document = load_compressed_data(201708388) #201801366 #201110421 #201903522
# print(document)


# import json

# # JSON 파일 경로
# file_path = 'all_codes_output.json'

# with open(file_path, 'r') as file:
#     data = json.load(file)

# # 'name'이 'John'인 항목의 'age' 값 찾기
# age_of_john = None
# for item in data:
#     if item['product_code'] == 201607314:
#         age_of_john = item['tag']
#         break

# if 'drug_container' in str(age_of_john) is not None:
#     print(f"John's age is: {age_of_john}")
# else:
#     print("John not found in the data.")