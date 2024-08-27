from crawl_pront_data import crawl_pront_data_to_csv
from crawl_product_code import crawl_product_code_to_csv
from crawl_popup_all_tag import crawl_popup_data_all_tag_to_json
from update_to_db import update_pront_data  , update_all_tag_data_zlib 
from update_to_db import update_popup_scroll_name
from db_control_05compressed import update_all_tag_data_compressed
import time
from pymongo import MongoClient
from crawl_last_page import get_last_page

if __name__ == "__main__":
    
    client = MongoClient('mongodb://localhost:27017')
    compress_db = client.medical_data_compressed
    compressed_parsing_db = client.parsing_data_compressed
    pront_data_col = compress_db['pront_data_col']
    # pront_data_col.drop()
    client.drop_database(compress_db)
    client.drop_database(compressed_parsing_db)
##############################################################################################################    
    start_time01 = time.time()
    start_time = time.time()
    
    print('start product_code crwaling')

    crawl_product_code_to_csv(6801 , get_last_page())
        
    print('start pront data crwaling')
    crawl_pront_data_to_csv(6801 , get_last_page())

    print('start popup data all tag crwaling')
    crawl_popup_data_all_tag_to_json()
    
    end_time = time.time()
    elapsed_time = end_time - start_time 
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

##############################################################################################################    
    start_time = time.time()
    
    print('start update pront data')
    update_pront_data()
    
    # print('start update popup_scroll name data')
    # update_popup_scroll_name()
    
    end_time = time.time()
    elapsed_time = end_time - start_time 
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")
# ##############################################################################################################    
    start_time = time.time()
    
    print('start update all tag data_zlib')
    update_all_tag_data_zlib()
    
    end_time = time.time()
    elapsed_time = end_time - start_time 
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")

##############################################################################################################
    start_time = time.time()
    
    client = MongoClient('mongodb://localhost:27017/')

    compress_db = client['medical_data_compressed']
    all_tag_doc = compress_db.all_tag_data_col.find()
    
    product_codes = all_tag_doc.distinct('product_code')
    
    print('start update popup page')
    update_all_tag_data_compressed(product_codes)
    
    end_time = time.time()
    elapsed_time = end_time - start_time 
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")
    
    end_time = time.time()
    elapsed_time = end_time - start_time01
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    
    print(f"All Process Elapsed time: {int(hours)}시 {int(minutes)}분 {int(seconds)}초")