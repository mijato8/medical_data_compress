import csv
import json
from pymongo import MongoClient
from bs4 import BeautifulSoup
import re
import time
from tqdm import tqdm
from tqdm.asyncio import tqdm
import pymongo
import zlib
from concurrent.futures import ThreadPoolExecutor

client = MongoClient('mongodb://localhost:27017/')
db = client['medical_data_compressed']
pront_data_col = db['pront_data']
popup_data_col = db['popup_data']
all_tag_data_col = db['all_tag_data']
popup_scroll_name_data_col = db['popup_scroll_name']

pront_data_path = 'pront_data.csv'
popup_data_path = 'popup_product_data.csv'
all_tag_data_path = 'all_codes_output.json'
popup_scroll_name_data_path = 'popup_scroll_name.csv'

####################################################################################################################
def update_row(row):
    product_code = row.get('품목기준코드')
    if product_code:
        db.pront_data_col.update_one({'품목기준코드': product_code}, {'$set': row}, upsert=True)

def update_pront_data():
    with open(pront_data_path, newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        rows = list(csv_reader)  # 모든 행을 미리 로드하여 총 행 수를 알 수 있게 함
        total_rows = len(rows)

        with ThreadPoolExecutor() as executor:
            list(tqdm(executor.map(update_row, rows), total=total_rows, desc="Updating database"))

    print("success update pront_data to database")
########################################################################################################################
def compress_data(data):
    return zlib.compress(json.dumps(data).encode('utf-8'))

def process_compressed_chunk(args):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = client.medical_data_compressed
    collection = db.all_tag_data_col
    row = args
    product_code = row.get('product_code')
    if product_code:
        compressed_data = compress_data(row)
        collection.update_one(
            {'product_code': product_code},
            {'$set': {'data': compressed_data}},
            upsert=True
        )
    else:
        print("Missing product_code, skipping row")

def update_all_tag_data_zlib():
    with open(all_tag_data_path, 'r', encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)

    with ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(process_compressed_chunk, data), total=len(data)))

    print("Successfully updated all tag data with compressed data")
####################################################################################################
def update_popup_scroll_name():
    all_tag_doc = db.all_tag_data_col.find()
    popup_scorll_name =[]
    for i in all_tag_doc:
        soup= BeautifulSoup(i['tag'] , 'html.parser')
        mid_tag = str(soup.find('div',class_='drug_info_mid'))
        mid_tag_name = re.findall(r'<div class="[^"]*scroll[^"]*" id="[^"]*">', mid_tag)
        scroll_name = dict({'product_code':i['product_code'], 'mid_tag_name':mid_tag_name})
        popup_scorll_name.append(scroll_name)
    for row in popup_scorll_name:
        # db.popup_scroll_name_data_col.insert_one(i)
        product_code = row.get('product_code')
        if product_code:
            db.popup_scroll_name_data_col.update_one({'product_code': product_code},{'$set': row},upsert=True)
    print("success update scroll name data to database")
    

if __name__ == "__main__":
    
    start_time = time.time()
    
    # update_pront_data()
    # # update_popup_data()
    # update_all_tag_data()
    # update_popup_scroll_name()
    

    end_time = time.time()
    elapsed_time = end_time - start_time 
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")