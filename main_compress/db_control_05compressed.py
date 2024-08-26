from pymongo import MongoClient
from bs4 import BeautifulSoup
import re
import time
import zlib
import json
from tqdm import tqdm
import io
import base64
import gzip
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
##########################################################################################################################################
##########################################################################################################################################
def product_info_scroll_01(soup , i , mid_tag_name):
    scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
    product_info = scroll_01_tag.find('table', class_ = 's-dr_table dr_table_type1')   # 기본정보
##########################################################################################################################################
    product_info_columns = product_info.find_all('th', {'scope': 'row'})   # 기본정보 columns
    product_info_col = [i.text for i in product_info_columns]
##########################################################################################################################################
    product_info2 = product_info.find_all('td')                             #기본정보 values
    product_info_values = [(i.text.replace('\n','')) for i in product_info2]
    if '*' in product_info_values[-1]:
        new_value = product_info_values[-2] + ' ' + product_info_values[-1]
        product_info_values[-2] = new_value
        product_info_values.pop(-1)
##########################################################################################################################################
    product_info_dic = dict(zip(product_info_col,product_info_values))      #기본정보 dict
    # print(product_info_dic)
##########################################################################################################################################    
    scroll_01_dict = {'product_code': i['product_code'] , 'product_info_data':product_info_dic}
    filter = {'product_code':scroll_01_dict['product_code']}
    update_data = {'$set':scroll_01_dict}
    compressed_parsing_db.product_info_scroll_01_col.update_one(filter , update_data , upsert=True)
    # print('product_info_scroll_01 update to db')
        
def mid_scroll_02(soup , i , mid_tag_name):
    
    def table_dict():
        mid_scroll_02_tag = soup.find('div', class_='info_sec notPkInfo scroll_02', id='scroll_02')
        table_cols_tag = mid_scroll_02_tag.find_all('th')                         # mid table col
        table_cols = [i.text for i in table_cols_tag]
        table_col = []
        table_column = []

        for item in table_cols:
            if item == '순번':
                if table_column:
                    table_col.append(table_column)
                table_column = [item]
            else:
                table_column.append(item)

        if table_column:
            table_col.append(table_column)

        table_values_tag = mid_scroll_02_tag.find_all('td')
        table_values = [i.text for i in table_values_tag]
        mid_col_len = len(table_col[0])
        table_values_list = [table_values[i:i+mid_col_len] for i in range(0, len(table_values), mid_col_len)]
        mid_table = [dict(zip(table_col[0], item)) for item in table_values_list]

        return mid_table
    
    if '<div class="info_sec notPkInfo scroll_02" id="scroll_02">' in mid_tag_name:
        mid_scroll_02_tag = soup.find('div', class_='info_sec notPkInfo scroll_02', id='scroll_02')    # mid_scroll_02_tag 
        tags_to_extract = [
            ('h3', "cont_title3 mt27 pb10"),
            ('table', 'tb_base'),
            ('h3', "cont_title4 mt3 pb10 pl10"),
            ('h3',"cont_title5 mt0 pb13 pl10")
        ]

        result = []

        for element in mid_scroll_02_tag.descendants:  # soup 내 모든 요소를 순회
            if isinstance(element, str):  # 텍스트 노드는 건너뜀
                continue
            for tag, class_name in tags_to_extract:
                # 클래스 목록을 문자열로 결합하여 비교
                if element.name == tag and " ".join(element.get('class', [])) == class_name:
                    result.append(element)  
                    break 

        mid_scroll02_data = []

        for k in result:
            if k.name == 'h3' and "cont_title3 mt27 pb10" in " ".join(k.get('class', [])):
                mid_scroll02_data.append(k.text)
            elif k.name == 'table' and "tb_base" in " ".join(k.get('class', [])):
                mid_scroll02_data.append(table_dict())
            elif k.name == 'h3' and "cont_title4 mt3 pb10 pl10" in " ".join(k.get('class', [])):
                mid_scroll02_data.append(k.text)
            elif k.name == 'h3' and "cont_title5 mt0 pb13 pl10" in " ".join(k.get('class', [])):
                mid_scroll02_data.append(k.text)
                mid_scroll02_data.append(f'https://nedrug.mfds.go.kr/resources/images/contents/attention.pdf')
                
        mid_scroll02_dict = {'product_code':i['product_code'],'mid_scroll_02_data':mid_scroll02_data}    
        # print(mid_scroll02_dict)
        filter = {'product_code':mid_scroll02_dict['product_code']}
        update_data = {'$set':mid_scroll02_dict}
        compressed_parsing_db.mid_scroll_02_col.update_one(filter , update_data , upsert=True)
        
def mid_scroll_03(soup , i , mid_tag_name):    
    if '<div class="info_sec _preview notPkInfo scroll_03" id="scroll_03">' in mid_tag_name:
        mid_scroll_03_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_03')     # mid_scroll_03 효능효과
        mid_scroll_03_info = mid_scroll_03_tag.find('div' , class_ = 'info_box').text.replace('\n','')
        mid_scroll_03_dict = {'product_code': i['product_code'] , 'mid_scroll_03_data':mid_scroll_03_info}
        # print(mid_scroll_03_dict)
        filter = {'product_code':mid_scroll_03_dict['product_code']}
        update_data = {'$set':mid_scroll_03_dict}
        compressed_parsing_db.mid_scroll_03_col.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_03 update to db')

def mid_scroll_04(soup_sub , i , mid_tag_name):
    if '<div class="info_sec _preview notPkInfo scroll_04" id="scroll_04">' in mid_tag_name:
        mid_scroll_04_tag = soup_sub.find('div' , class_='info_sec _preview notPkInfo scroll_04')
        tables = mid_scroll_04_tag.find_all('table', class_='indent')
        
        for index, table in enumerate(tables):
            parent = table.parent
            new_text = soup_sub.new_string(f'mid_scroll_04_table{index}')
            table.decompose()
            parent.insert_before(new_text)
        
        mid_scroll_04_info = mid_scroll_04_tag.find('div' , class_ = 'info_box mt20 pt0').text                 # mid_scroll_04 용법용량
        # print(mid_scroll_04_info)
        mid_scroll_04_dict = {'product_code': i['product_code'] , 'mid_scroll_04_data':mid_scroll_04_info}
        filter = {'product_code':mid_scroll_04_dict['product_code']}
        update_data = {'$set':mid_scroll_04_dict}
        compressed_parsing_db.mid_scroll_04_col.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_04 update to db')

def mid_scroll_05(soup_sub01 , i , mid_tag_name):
    if '<div class="info_sec _preview notPkInfo scroll_05" id="scroll_05">' in mid_tag_name:
        mid_scroll_05_tag = soup_sub01.find('div' , class_='info_sec _preview notPkInfo scroll_05')
        tables = mid_scroll_05_tag.find_all('table', class_='indent')

        for index, table in enumerate(tables):
            parent = table.parent
            new_text = soup_sub01.new_string(f'mid_scoll_05_table{index}')
            table.decompose()
            parent.insert_before(new_text)
        
        mid_scroll_05_info = mid_scroll_05_tag.find('div', class_ = 'info_box mt30 pt0 notice').text          # mid_scroll_05 사용상의주의사항
        # print(mid_scroll_05_info)
        mid_scroll_05_dict = {'product_code': i['product_code'] , 'mid_scroll_05_data':mid_scroll_05_info}
        filter = {'product_code':mid_scroll_05_dict['product_code']}
        update_data = {'$set':mid_scroll_05_dict}
        compressed_parsing_db.mid_scroll_05_col.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_05 update to db')
        
def mid_scroll_06(soup , i , mid_tag_name):
    
    if '<div class="info_sec notPkInfo scroll_06" id="scroll_06">' in mid_tag_name:
        mid_scroll_06_tag = soup.find('div',class_="info_sec notPkInfo scroll_06")    #mid_scroll_06 dur정보
        mid_scroll_06_info = mid_scroll_06_tag.find('table' , class_ = 's-dr_table dr_table dr_table_type2')
        if mid_scroll_06_tag:
            mid_scroll_06_cols = mid_scroll_06_info.find_all('th', {'scope': 'col'})
            mid_scroll_06_col = [i.text for i in mid_scroll_06_cols]
            mid_scroll_06_values = mid_scroll_06_info.find_all('td')
            mid_scroll_06_find_value = []
        for tag in mid_scroll_06_values:
            spans_and_links = tag.find_all(['span', 'a'])
            for element in spans_and_links:
                if 's-th' not in element.get('class', []):
                    mid_scroll_06_find_value.append(element.text.strip())
            for span in tag.find_all('span', class_='s-th'):
                sibling_text = span.next_sibling
                if sibling_text and isinstance(sibling_text, str):
                    sibling_text = sibling_text.strip()
                    if sibling_text:  # 빈 문자열이 아닌 경우에만 추가
                        mid_scroll_06_find_value.append(sibling_text)
    
        col_length = len(mid_scroll_06_col)
        parts = []
        current_part = []
        
        for item in mid_scroll_06_find_value:
            if item in ['단일' , '복합' , '분할주의']:  #??????
                if current_part:
                    parts.append(current_part)
                current_part = [item]
            else:
                current_part.append(item)

        if current_part:
            parts.append(current_part)

        mid_scroll_06_value = []
        for part in parts:
            if len(part) < col_length:
                part.extend([''] * (col_length - len(part)))
            mid_scroll_06_value.extend(part)
        
        mid_scroll_06_col = mid_scroll_06_col * int(len(mid_scroll_06_value)/len(mid_scroll_06_col))   #mid_scroll_06 cols
        
        col_index = []
        if '단일/복합' in mid_scroll_06_col :
            for j,k in enumerate(mid_scroll_06_col):
                if k == '단일/복합':
                    col_index.append(j)          
        else:
            for j,k in enumerate(mid_scroll_06_col):
                if k == 'DUR유형':
                    col_index.append(j)
                                        
        # print(i['product_code'])
        split_value = [mid_scroll_06_value[col_index[i]:col_index[i+1]] for i in range(len(col_index) - 1)]
        split_value.append(mid_scroll_06_value[col_index[-1]:])
        mid_scroll_06_data_01 = [[{mid_scroll_06_col[j]: split_value[i][j - col_index[i]] for j in range(col_index[i], col_index[i] + len(split_value[i]))}] for i in range(len(split_value))]
        mid_scroll_06_data = []
        for p in mid_scroll_06_data_01:
            for j in p:
                mid_scroll_06_data.append(j)
        mid_scroll_06_dict = {'product_code': i['product_code'] , 'mid_scroll_06_data':mid_scroll_06_data}
        # print(mid_scroll_06_dict)
        filter = {'product_code':mid_scroll_06_dict['product_code']}
        update_data = {'$set':mid_scroll_06_dict}
        compressed_parsing_db.mid_scroll_06_col.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_06 update to db')
            
def mid_scroll_07(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_07" id="scroll_07">' in mid_tag_name:
        mid_scroll_07_tag = soup.find('div' , class_='info_sec notPkInfo scroll_07')
        mid_scroll_07_info = mid_scroll_07_tag.find('table', class_ = 's-dr_table dr_table_type2 s-view-table ss_table') #mid_scroll_07 재심사, RMP, 보험, 기타정보
        # print(mid_scroll_07_info)
        mid_scroll_07_cols = mid_scroll_07_info.find_all('th', {'scope': 'row'})
        mid_scroll_07_col = [i.text for i in mid_scroll_07_cols]
        # print(mid_scroll_07_col)
        mid_scroll_07_values = mid_scroll_07_info.find_all('td')
        mid_scroll_07_value = [i.text.replace('\n','') for i in mid_scroll_07_values]
        mid_scroll_07_data = dict(zip(mid_scroll_07_col,mid_scroll_07_value))
        # print(mid_scroll_07_data)
        mid_scroll_07_dict = {'product_code': i['product_code'] , 'mid_scroll_07_data':mid_scroll_07_data}
        # print(mid_scroll_06_dict)
        filter = {'product_code':mid_scroll_07_dict['product_code']}
        update_data = {'$set':mid_scroll_07_dict}
        compressed_parsing_db.mid_scroll_07_col.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_07 update to db')

def mid_scroll_08(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_08" id="scroll_08">' in mid_tag_name:
        mid_scroll_08_tag = soup.find('div' , class_='info_sec notPkInfo scroll_08')
        mid_scroll_08_info = mid_scroll_08_tag.find('table' , class_ = 's-dr_table dr_table_type2')    # mid_scroll_08 생산실적
        if mid_scroll_08_info:
            mid_scroll_08_cols = mid_scroll_08_info.find_all('th', {'scope': 'col'})
            mid_scroll_08_col = [i.text for i in mid_scroll_08_cols]
            mid_scroll_08_values = mid_scroll_08_info.find_all('td')
            mid_scroll_08_value = [i.text for i in mid_scroll_08_values]                                    #mid_scroll_08 values
            mid_scroll_08_col = mid_scroll_08_col * int(len(mid_scroll_08_value)/len(mid_scroll_08_col))   #mid_scroll_08 cols
            col_index = [j for j, i in enumerate(mid_scroll_08_col) if i == '년도']
            split_value = [mid_scroll_08_value[col_index[i]:col_index[i+1]] for i in range(len(col_index) - 1)]
            split_value.append(mid_scroll_08_value[col_index[-1]:])
            mid_scroll_08_dict_01 = [[{mid_scroll_08_col[j]: split_value[i][j - col_index[i]] for j in range(col_index[i], col_index[i] + len(split_value[i]))}] for i in range(len(split_value))]
            mid_scroll_08_data = []
            for p in mid_scroll_08_dict_01:
                for q in p:
                    mid_scroll_08_data.append(q)
            mid_scroll_08_dict = {'product_code': i['product_code'] , 'mid_scroll_08_data':mid_scroll_08_data}
            # print(mid_scroll_08_dict)
            filter = {'product_code':mid_scroll_08_dict['product_code']}
            update_data = {'$set':mid_scroll_08_dict}
            compressed_parsing_db.mid_scroll_08_col.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_08 update to db')

def mid_scroll_09(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_09" id="scroll_09">' in mid_tag_name:
        mid_scroll_09_tag = soup.find('div',class_="info_sec notPkInfo scroll_09")    #mid_scroll_99 e약은요
        mid_scroll_09_info = mid_scroll_09_tag.find('table' , class_ = 's-dr_table dr_table_type2')
        if mid_scroll_09_tag:
            mid_scroll_09_cols = mid_scroll_09_info.find_all('th', {'scope': 'col'})
            mid_scroll_09_col = [i.text for i in mid_scroll_09_cols]
            mid_scroll_09_values = mid_scroll_09_info.find_all('td')
            mid_scroll_09_value = [i.text for i in mid_scroll_09_values]                                    #mid_scroll_08 values
            mid_scroll_09_col = mid_scroll_09_col * int(len(mid_scroll_09_value)/len(mid_scroll_09_col))   #mid_scroll_08 cols
            col_index = [j for j, i in enumerate(mid_scroll_09_col) if i == '년도']
            split_value = [mid_scroll_09_value[col_index[i]:col_index[i+1]] for i in range(len(col_index) - 1)]
            split_value.append(mid_scroll_09_value[col_index[-1]:])
            mid_scroll_09_data_01 = [[{mid_scroll_09_col[j]: split_value[i][j - col_index[i]] for j in range(col_index[i], col_index[i] + len(split_value[i]))}] for i in range(len(split_value))]
            mid_scroll_09_data = []
            for p in mid_scroll_09_data_01:
                for j in p:
                    mid_scroll_09_data.append(j)
            mid_scroll_09_dict = {'product_code': i['product_code'] , 'mid_scroll_09_data':mid_scroll_09_data}
            # print(mid_scroll_09_dict)
            filter = {'product_code':mid_scroll_09_dict['product_code']}
            update_data = {'$set':mid_scroll_09_dict}
            compressed_parsing_db.mid_scroll_09_col.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_09 update to db')
            
def mid_scroll_10(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_10" id="scroll_10">' in mid_tag_name:
        mid_scroll_10_tag = soup.find('div' , class_='info_sec notPkInfo scroll_10')
        mid_scroll_10_table = mid_scroll_10_tag.find('table' , class_ = 's-dr_table dr_table dr_table_type2')   #mid_scroll_10 변경이력
        if mid_scroll_10_table:
            mid_scroll_10_cols = mid_scroll_10_table.select('span:nth-of-type(1)')   #mid_scroll_10 column
            mid_scroll_10_col = [i.text for i in mid_scroll_10_cols][1:]

            mid_scroll_10_values = mid_scroll_10_table.select('span:nth-of-type(2)')   #mid_scroll_10 value
            mid_scroll_10_value = [i.text for i in mid_scroll_10_values]
##############################################################################################################################################################################
            mid_scroll_10_col_index = [j for j,i in enumerate(mid_scroll_10_col) if i == '순번']                                 #mid_scroll_10 dict
            split_value = [mid_scroll_10_value[mid_scroll_10_col_index[i]:mid_scroll_10_col_index[i+1]] for i in range(len(mid_scroll_10_col_index) - 1)]
            split_value.append(mid_scroll_10_value[mid_scroll_10_col_index[-1]:])
            mid_scroll_10_data_01 = [[{mid_scroll_10_col[j]: part[j] for j in range(len(part))}] for part in split_value]
            mid_scroll_10_data = []
            for p in mid_scroll_10_data_01:
                for j in p:
                    mid_scroll_10_data.append(j)
            mid_scroll_10_dict = {'product_code': i['product_code'] , 'mid_scroll_10_data':mid_scroll_10_data}
            # print(mid_scroll_10_dict)
            filter = {'product_code':mid_scroll_10_dict['product_code']}
            update_data = {'$set':mid_scroll_10_dict}
            compressed_parsing_db.mid_scroll_10_col.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_10 update to db')

def mid_scroll_12(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_12" id="scroll_12">' in mid_tag_name:
        mid_scroll_12_tag = soup.find('div',class_="info_sec notPkInfo scroll_12")    #mid_scroll_12 특허정보
        mid_scroll_12_info = mid_scroll_12_tag.find('table' , class_ = 's-dr_table dr_table dr_table_type2')
        if mid_scroll_12_tag:
            mid_scroll_12_cols = mid_scroll_12_info.find_all('th', {'scope': 'col'})
            mid_scroll_12_col = [i.text for i in mid_scroll_12_cols]
            mid_scroll_12_values = mid_scroll_12_info.find_all('td')
            mid_scroll_12_value = []
            for tag in mid_scroll_12_values:
                spans_and_links = tag.find_all(['span', 'a'])
                for element in spans_and_links:
                    if 's-th' not in element.get('class', []):
                        mid_scroll_12_value.append(element.text)
            # print(mid_scroll_12_value)                                  #mid_scroll_12 values
            mid_scroll_12_col = mid_scroll_12_col * int(len(mid_scroll_12_value)/len(mid_scroll_12_col))   #mid_scroll_12 cols
            # print(mid_scroll_12_col)
            col_index = [j for j, i in enumerate(mid_scroll_12_col) if i == '순번']
            split_value = [mid_scroll_12_value[col_index[i]:col_index[i+1]] for i in range(len(col_index) - 1)]
            split_value.append(mid_scroll_12_value[col_index[-1]:])
            mid_scroll_12_data_01 = [[{mid_scroll_12_col[j]: split_value[i][j - col_index[i]] for j in range(col_index[i], col_index[i] + len(split_value[i]))}] for i in range(len(split_value))]
            mid_scroll_12_data = []
            for p in mid_scroll_12_data_01:
                for j in p:
                    mid_scroll_12_data.append(j)
            mid_scroll_12_dict = {'product_code': i['product_code'] , 'mid_scroll_12_data':mid_scroll_12_data}
            # print(mid_scroll_12_dict)
            filter = {'product_code':mid_scroll_12_dict['product_code']}
            update_data = {'$set':mid_scroll_12_dict}
            compressed_parsing_db.mid_scroll_12_col.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_12 update to db')

def mid_scroll_13(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_13" id="scroll_13">' in mid_tag_name:
        mid_scroll_13_tag = soup.find('div',class_="info_sec notPkInfo scroll_13")    #mid_scroll_13 특허정보
        mid_scroll_13_info = mid_scroll_13_tag.find('table' , class_ = 's-dr_table dr_table dr_table_type2')
        if mid_scroll_13_tag:
            mid_scroll_13_cols = mid_scroll_13_info.find_all('th', {'scope': 'col'})
            mid_scroll_13_col = [i.text for i in mid_scroll_13_cols]
            mid_scroll_13_values = mid_scroll_13_info.find_all('td')
            mid_scroll_13_value = [i.text.replace('\n','') for i in mid_scroll_13_values]
            # print(mid_scroll_13_value)                                                                #mid_scroll_13 values
            mid_scroll_13_col = mid_scroll_13_col * int(len(mid_scroll_13_value)/len(mid_scroll_13_col))   #mid_scroll_13 cols
            # print(mid_scroll_13_col)
            col_index = [j for j, i in enumerate(mid_scroll_13_col) if i == '순번']
            split_value = [mid_scroll_13_value[col_index[i]:col_index[i+1]] for i in range(len(col_index) - 1)]
            split_value.append(mid_scroll_13_value[col_index[-1]:])
            mid_scroll_13_data_01 = [[{mid_scroll_13_col[j]: split_value[i][j - col_index[i]] for j in range(col_index[i], col_index[i] + len(split_value[i]))}] for i in range(len(split_value))]
            mid_scroll_13_data = []
            for p in mid_scroll_13_data_01:
                for j in p:
                    mid_scroll_13_data.append(j)
            mid_scroll_13_dict = {'product_code': i['product_code'] , 'mid_scroll_13_data':mid_scroll_13_data}
            # print(mid_scroll_13_dict)
            filter = {'product_code':mid_scroll_13_dict['product_code']}
            update_data = {'$set':mid_scroll_13_dict}
            compressed_parsing_db.mid_scroll_13_col.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_13 update to db')
                
def mid_scroll_99(soup , i , mid_tag_name):
    if '<div class="info_sec _preview notPkInfo scroll_99" id="scroll_99">' in mid_tag_name:
        mid_scroll_99_tag = soup.find('div',class_="info_sec _preview notPkInfo scroll_99")    #mid_scroll_99 e약은요
        if mid_scroll_99_tag:
            mid_scroll_99_info = mid_scroll_99_tag.find('div',class_ = 'info_box').text.strip()
            # print(mid_scroll_99_info)
            mid_scroll_99_dict = {'product_code': i['product_code'] , 'mid_scroll_99_data':mid_scroll_99_info}
            filter = {'product_code':mid_scroll_99_dict['product_code']}
            update_data = {'$set':mid_scroll_99_dict}
            compressed_parsing_db.mid_scroll_99_col.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_99 update to db')

def compress_image(image_data):
    """Base64로 인코딩된 이미지를 디코딩하고 압축하여 반환합니다."""
    # Base64 데이터에서 이미지 데이터를 추출합니다.
    header, encoded = image_data.split(',', 1)
    image_bytes = base64.b64decode(encoded)
    
    # 이미지를 gzip으로 압축합니다.
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as f:
        f.write(image_bytes)
    
    compressed_data = buf.getvalue()
    return compressed_data

def product_info_img(soup, i, mid_tag_name):
    scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
    if scroll_01_tag.find('div', class_='pc-img'):
        scroll_01_div = scroll_01_tag.find('div', class_='pc-img')
        scroll_01_img = scroll_01_div.find_all('img')
    
        if scroll_01_img:
            scroll_01_img_src = [img['src'] for img in scroll_01_img]
            compressed_images = [compress_image(src) for src in scroll_01_img_src]
            
            scroll_01_img_src_dic = {
                'product_code': i['product_code'],
                'img': compressed_images
            }
            
            filter = {'product_code': scroll_01_img_src_dic['product_code']}
            update_data = {'$set': scroll_01_img_src_dic}
            compressed_parsing_db.scroll_01_img.update_one(filter, update_data, upsert=True)

def scroll_01_link_01(soup , i , mid_tag_name):
    scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
    product_info = scroll_01_tag.find('table', class_ = 's-dr_table dr_table_type1')
    product_info_link = product_info.find('button',class_='btn_small btn_base btn_point3')['onclick']
    file_id = re.search(r"'(.*?)'", product_info_link).group(1)
    filepath = f'https://nedrug.mfds.go.kr{file_id}'
    scroll_01_link_path = {'product_code':i['product_code'] , 'scroll_01_link_path':filepath}
    filter = {'product_code':scroll_01_link_path['product_code']}
    update_data = {'$set':scroll_01_link_path}
    compressed_parsing_db.scroll_01_link_01.update_one(filter , update_data , upsert=True)
    # print('product_info_scroll_01_link_01 update to db')

def scroll_01_link_02(soup , i , mid_tag_name):
    scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
    product_info = scroll_01_tag.find('table', class_ = 's-dr_table dr_table_type1')   
    if 'btn_small btn_base btn_point4' in str(product_info):
        scroll_01_link = product_info.find('button' , class_='btn_small btn_base btn_point4')['onclick']
        file_id = re.search(r"'(.*?)'", scroll_01_link).group(1)
        filepath = f'https://nedrug.mfds.go.kr{file_id}'
        scroll_01_link_path_02 = {'product_code':i['product_code'] , 'scroll_01_button_path':filepath}
        filter = {'product_code':scroll_01_link_path_02['product_code']}
        update_data = {'$set':scroll_01_link_path_02}
        compressed_parsing_db.scroll_01_link_02.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_01_link_02 update to db')

def scroll_01_link_03(soup , i , mid_tag_name):
    scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
    product_info = scroll_01_tag.find('table', class_ = 's-dr_table dr_table_type1')   
    if 'btn_small btn_base btn_point2' in str(product_info):
        scroll_01_link = product_info.find_all('button' , class_='btn_small btn_base btn_point2')  #['onclick']
        scroll_01_links_03 = []
        for k in scroll_01_link:
            file_id = re.search(r"'(.*?)'", k['onclick']).group(1)
            filepath = f'https://nedrug.mfds.go.kr{file_id}'
            scroll_01_links_03.append(filepath)
        scroll_01_link_path_03 = {'product_code':i['product_code'] , 'scroll_01_button_path':scroll_01_links_03}
        filter = {'product_code':scroll_01_link_path_03['product_code']}
        update_data = {'$set':scroll_01_link_path_03}
        compressed_parsing_db.scroll_01_link_03.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_01_link_03 update to db')

def scroll_01_button_path(soup , i , mid_tag_name):
    scroll_01_tag = soup.find('div', class_=['drug_info_top', 'notPkInfo', 'not_button'])
    product_info = scroll_01_tag.find('table', class_ = 's-dr_table dr_table_type1')
    if 'btn_small btn_icon' in str(product_info):
        scroll_01_doc = product_info.find('button' , class_='btn_small btn_icon')['onclick']
        file_id = re.search(r"'(.*?)'", scroll_01_doc).group(1)
        filepath = f'https://nedrug.mfds.go.kr/cmn/edms/down/{file_id}'
        scroll_01_button_path = {'product_code':i['product_code'] , 'scroll_01_button_path':filepath}
        filter = {'product_code':scroll_01_button_path['product_code']}
        update_data = {'$set':scroll_01_button_path}
        compressed_parsing_db.scroll_01_button_path.update_one(filter , update_data , upsert=True)
        # print('product_info_scroll_01_button_path update to db')

def scroll_04_table(soup , i , mid_tag_name):
    if '<div class="info_sec _preview notPkInfo scroll_04" id="scroll_04">' in mid_tag_name:
        mid_scroll_04_tag = soup.find('div' , class_='info_sec _preview notPkInfo scroll_04')
        if mid_scroll_04_tag.find_all('table', class_='indent'):
            mid_scroll_04_indent = mid_scroll_04_tag.find_all('table', class_='indent')
            mid_scroll_04_table = [str(k) for k in mid_scroll_04_indent]
            mid_scroll_04_table_dict = {'product_code': i['product_code'] , 'mid_scroll_04_table':mid_scroll_04_table}
            filter = {'product_code':mid_scroll_04_table_dict['product_code']}
            update_data = {'$set':mid_scroll_04_table_dict}
            compressed_parsing_db.mid_scroll_04_table.update_one(filter , update_data , upsert=True)

def scroll_05_table(soup , i , mid_tag_name):
    if '<div class="info_sec _preview notPkInfo scroll_05" id="scroll_05">' in mid_tag_name:
        mid_scroll_05_tag_01 = soup.find('div' , class_='info_sec _preview notPkInfo scroll_05')
        if mid_scroll_05_tag_01.find_all('table', class_='indent'):
            mid_scroll_05_indent = mid_scroll_05_tag_01.find_all('table', class_='indent')
            mid_scroll_05_table = [str(k) for k in mid_scroll_05_indent]
            mid_scroll_05_table_dict = {'product_code': i['product_code'] , 'mid_scroll_05_table':mid_scroll_05_table}
            filter = {'product_code':mid_scroll_05_table_dict['product_code']}
            update_data = {'$set':mid_scroll_05_table_dict}
            compressed_parsing_db.mid_scroll_05_table.update_one(filter , update_data , upsert=True)

def scroll_07_button_path(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_07" id="scroll_07">' in mid_tag_name:
        mid_scroll_07_tag = soup.find('div' , class_='info_sec notPkInfo scroll_07')
        mid_scroll_07_info = mid_scroll_07_tag.find('table', class_ = 's-dr_table dr_table_type2 s-view-table ss_table')
        if 'btn_small btn_base btn_point2 ml20' in str(mid_scroll_07_info):
            scroll_07_button = mid_scroll_07_info.find('button' , class_='btn_small btn_base btn_point2 ml20')['onclick']
            file_id = re.search(r"'(.*?)'", scroll_07_button).group(1)
            filepath = f'https://nedrug.mfds.go.kr{file_id}'
            scroll_07_button_path = {'product_code':i['product_code'] , 'scroll_07_button_path':filepath}
            filter = {'product_code':scroll_07_button_path['product_code']}
            update_data = {'$set':scroll_07_button_path}
            compressed_parsing_db.scroll_07_button_path.update_one(filter , update_data , upsert=True)
            # print('product_info_scroll_07_button_path update to db')

def scroll_12_link_01(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_12" id="scroll_12">' in mid_tag_name:
        scroll_12_tag = soup.find('div', class_='info_sec notPkInfo scroll_12')
        scroll_12_table = scroll_12_tag.find('table', class_ = 's-dr_table dr_table dr_table_type2')
        scroll_12_href = scroll_12_table.find_all('a')
        if scroll_12_href:
            scroll_12_link = [i['href'] for i in scroll_12_href if '상세보기' not in i]
            if len(scroll_12_link) != 0 :
                scroll_12_link_dic = {'product_code':i['product_code'] , 'scroll_12_link_01': scroll_12_link}
                filter = {'product_code':scroll_12_link_dic['product_code']}
                update_data = {'$set':scroll_12_link_dic}
                compressed_parsing_db.scroll_12_link_01.update_one(filter , update_data , upsert=True)
                # print('product_info_scroll_12_link_01 update to db')
                
def scroll_12_link_02(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_12" id="scroll_12">' in mid_tag_name:
        scroll_12_tag = soup.find('div', class_='info_sec notPkInfo scroll_12')
        scroll_12_table = scroll_12_tag.find('table', class_ = 's-dr_table dr_table dr_table_type2')
        scroll_12_href = scroll_12_table.find_all('a')
        if scroll_12_href:
            scroll_12_link = [f'https://nedrug.mfds.go.kr/'+i['href'] for i in scroll_12_href if '상세보기' in i]
            if len(scroll_12_link) != 0 :
                scroll_12_link_dic = {'product_code':i['product_code'] , 'scroll_12_link_02': scroll_12_link}
                filter = {'product_code':scroll_12_link_dic['product_code']}
                update_data = {'$set':scroll_12_link_dic}
                compressed_parsing_db.scroll_12_link_02.update_one(filter , update_data , upsert=True)
                # print('product_info_scroll_12_link_02 update to db')
        
def scroll_13_button_path(soup , i , mid_tag_name):
    if '<div class="info_sec notPkInfo scroll_13" id="scroll_13">' in mid_tag_name:
        scroll_13_tag = soup.find('div', class_='info_sec notPkInfo scroll_13')
        scroll_13_table = scroll_13_tag.find('table', class_ = 's-dr_table dr_table dr_table_type2')
        if 'btn_small btn_icon' in str(scroll_13_table):
            scroll_13_doc = scroll_13_table.find_all('button' , class_='btn_small btn_icon')
            filepath=[]
            for k in scroll_13_doc:
                file_id = (re.search(r"'(.*?)'", k['onclick']).group(1))
                path = f'https://nedrug.mfds.go.kr/cmn/edms/down/{file_id}'
                filepath.append(path)
                scroll_13_button_path = {'product_code':i['product_code'] , 'scroll_13_button_path':filepath}
                filter = {'product_code':scroll_13_button_path['product_code']}
                update_data = {'$set':scroll_13_button_path}
                compressed_parsing_db.scroll_13_button_path.update_one(filter , update_data , upsert=True)
                # print('product_info_scroll_13_button_path update to db')

##########################################################################################################################################
##########################################################################################################################################
def decompress_data(compressed_data):
    try:
        return json.loads(zlib.decompress(compressed_data).decode('utf-8'))
    except (zlib.error, json.JSONDecodeError) as e:
        print(f"Error decompressing data: {e}")
        return None

def load_compressed_data(product_code):
    client = MongoClient('mongodb://localhost:27017/')
    compress_db = client['medical_data_compressed']
    collection = compress_db['all_tag_data_col']
    
    # 필요 필드만 가져오기
    document = collection.find_one({'product_code': product_code}, {'data': 1})
    if document and 'data' in document:
        compressed_data = document['data']
        return decompress_data(compressed_data)
    else:
        raise ValueError(f"No data found for product_code '{product_code}'")

def process_document(document):
    if document is None:
        return
    soup = BeautifulSoup(document['tag'], 'html.parser')
    soup_sub = BeautifulSoup(document['tag'], 'html.parser')
    soup_sub01 = BeautifulSoup(document['tag'], 'html.parser')
    mid_tag_name = re.findall(r'<div class="[^"]*scroll[^"]*" id="[^"]*">', str(soup))

    product_info_scroll_01(soup, document, mid_tag_name)
    mid_scroll_02(soup, document, mid_tag_name)
    mid_scroll_03(soup, document, mid_tag_name)
    mid_scroll_04(soup_sub, document, mid_tag_name)
    mid_scroll_05(soup_sub01, document, mid_tag_name)
    mid_scroll_06(soup, document, mid_tag_name)
    mid_scroll_07(soup, document, mid_tag_name)
    mid_scroll_08(soup, document, mid_tag_name)
    mid_scroll_09(soup, document, mid_tag_name)
    mid_scroll_10(soup, document, mid_tag_name)
    mid_scroll_12(soup, document, mid_tag_name)
    mid_scroll_13(soup, document, mid_tag_name)
    mid_scroll_99(soup, document, mid_tag_name)
    product_info_img(soup, document, mid_tag_name)
    scroll_01_link_01(soup, document, mid_tag_name)
    scroll_01_link_02(soup, document, mid_tag_name)
    scroll_01_link_03(soup, document, mid_tag_name)
    scroll_01_button_path(soup, document, mid_tag_name)
    scroll_04_table(soup, document, mid_tag_name)
    scroll_05_table(soup, document, mid_tag_name)
    scroll_07_button_path(soup, document, mid_tag_name)
    scroll_12_link_01(soup, document, mid_tag_name)
    scroll_12_link_02(soup, document, mid_tag_name)
    scroll_13_button_path(soup, document, mid_tag_name)
        
def process_product_code(product_code):
    try:
        document = load_compressed_data(product_code)
        process_document(document)
    except ValueError as e:
        print(e)

def update_all_tag_data_compressed(product_codes):
    max_workers = int(os.cpu_count() * 2)
    with ProcessPoolExecutor(max_workers = max_workers) as executor:
        futures = {executor.submit(process_product_code, code): code for code in product_codes}
        for future in tqdm(as_completed(futures), total=len(futures), desc="update popup page"):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing product_code {futures[future]}: {e}")
    
client = MongoClient('mongodb://localhost:27017/')
compress_db = client['medical_data_compressed']
compressed_parsing_db = client['parsing_data_compressed']

pront_data_col = compress_db['pront_data']
popup_data_col = compress_db['popup_data']
all_tag_data_col = compress_db['all_tag_data']
popup_scroll_name_data_col = compress_db['popup_scroll_name']

if __name__ == "__main__":

    start_time = time.time()

    # 메모리에 캐싱하여 product_code 리스트를 로드
    all_tag_doc = compress_db.all_tag_data_col.find({}, {'product_code': 1})
    product_codes = [doc['product_code'] for doc in all_tag_doc]

    update_all_tag_data_compressed(product_codes)

    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")
    
##################################################################################################################################
