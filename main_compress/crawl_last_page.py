import requests
from bs4 import BeautifulSoup
import re

def get_last_page():
    url = 'https://nedrug.mfds.go.kr/searchDrug'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    last_page_button = soup.find('button', class_='page_last')
    if not last_page_button:
        return None

    onclick_attr = last_page_button.get('onclick')
    if not onclick_attr:
        return None

    # 정규 표현식을 사용하여 숫자를 추출
    match = re.search(r'getList1\((\d+)\)', onclick_attr)
    if not match:
        return None

    last_page_number = int(match.group(1))
    
    return last_page_number
    

# print(f'Last page number is: {last_page_number}')
# print(get_last_page(url))

