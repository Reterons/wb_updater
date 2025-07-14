import os
import requests
import pandas as pd
from io import StringIO
import re
import csv
from dotenv import load_dotenv
from time import sleep
import logging
import json
from datetime import datetime, timedelta
import numpy as np

WB_WAREHOUSE_ID = "1030859"
WB_API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2ODAxNjk0MCwiaWQiOiIwMTk3ZmEyYy1mOGEzLTdiNzgtYTJmOS1mNjM1ZTE5ZjRkYmMiLCJpaWQiOjEwMTg5NzM0LCJvaWQiOjM5NzQ3MTQsInMiOjE2MTI2LCJzaWQiOiIyMDBjMWJmMC0xM2UxLTQ4MmEtYTM1MS01NjlhNzgxN2NiMmQiLCJ0IjpmYWxzZSwidWlkIjoxMDE4OTczNH0.FUZNpgyRFT7HTar-l860MUJGr2WBVRvcoVmU9n72-t22NmSeeKqeRa5Mmn7I7zB0WU-P-IWpDnDjpp0mNpo3yQ"
headers = {
    "Authorization": WB_API_KEY,
    "Content-Type": "application/json"
}
STOCKS_URL = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{WB_WAREHOUSE_ID}"
SUPPLIER_URL = "https://shop.firma-gamma.ru/api/v1.0/stock.php?type=csv"

def get_supplier_data():
    response = requests.get(
        SUPPLIER_URL,
        auth=requests.auth.HTTPBasicAuth("natalia-b2005", "Holst110878"),
    )
    response.raise_for_status()
    content = response.content.decode("windows-1251", errors='replace')
    column_names = [
        'id',
        'conversion_factor',
        'wholesale_price_retail_pack',
        'min_recommended_retail_price',
        'name',
        'availability_status',
        'wholesale_price_wholesale_pack'
    ]
    df = pd.read_csv(
        StringIO(content),  
        sep='\t',            
        header=None,                
        names=column_names,           
        quoting=csv.QUOTE_MINIMAL,     
        doublequote=True,              
        escapechar=None,               
    )
    return df

def process_availability(df):
    new_coll = []
    tags = ["в наличии", "под заказ 1-5 дней", "остаток", "под заказ 1-2 дня (остаток), под заказ 1-5 дней", "под заказ 1-5 дней (остаток)", "под заказ 1-5 дней(остаток)"]
    for i in df["availability_status"]:
        if i in tags:
            new_coll.append(30)
        else:
            new_coll.append(0)
    return df.assign(counts=new_coll)

def filter_by_brands(df):
    url = "https://docs.google.com/spreadsheets/d/15OM5gynhTPDGOqoKounaoUGS7Y40Xc5JXBbfyFE22fs/export?format=csv"
    brands = pd.read_csv(url)
    first_column = brands.iloc[:, 0]
    new_tags = []
    for i in first_column:
        new_tags.append(i)
    pattern = r'\b(?:' + '|'.join(map(re.escape, new_tags)) + r')\b'
    mask = df['name'].str.contains(pattern, case=False, regex=True, na=False)
    return df[mask].copy()

def get_all_cards(api_key):
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    
    all_cards = []
    limit = 100  
    cursor = None  
    
    while True:
        sleep(5)
       
        payload = {
            "settings": {
                "cursor": {
                    "limit": limit
                },
                "filter": {
                    "withPhoto": -1  
                }
            }
        }
        
        if cursor:
            payload["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
            payload["settings"]["cursor"]["nmID"] = cursor["nmID"]
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"Ошибка {response.status_code}: {response.text}")
            break
        
        data = response.json()
        cards = data.get("cards", [])
        total = data.get("total", 0)
        
        if not cards:
            break  
        
        all_cards.extend(cards)
        print(f"Загружено: {len(all_cards)} / {total}")
        
        if len(cards) < limit:
            break
        
        last_card = cards[-1]
        cursor = {
            "updatedAt": last_card["updatedAt"],
            "nmID": last_card["nmID"]
        }
    
    return all_cards

def get_chrt_ids_by_imt_id(api_key, imt_id, all_cards):
    chrt_id = 0
    for card in all_cards:
        if str(card["vendorCode"]) == str(imt_id):
            chrt_id = card["sizes"][0]["skus"][0]
    return chrt_id

def prepare_products_data(new_df, all_cards):
    products = new_df.groupby('id').agg({
        'counts': 'sum',
        'availability_status': 'first'
    }).reset_index()
        
    stocks_data = []
    barcodes = []
    
    for i in products["id"]:
        chrt = get_chrt_ids_by_imt_id(WB_API_KEY, i, all_cards)
        if chrt:
            barcodes.append(chrt)
        else:
            barcodes.append(np.nan)
    
    products = products.assign(barcode=barcodes)
    products = products.dropna()
    
    for _, row in products.iterrows():
        sku = str(row['barcode'])
        amount = int(row['counts'])
        stocks_data.append({
            "sku": sku,
            "amount": amount,
        })
    
    return stocks_data

def send_data(data):
    try:
        response = requests.put(
            STOCKS_URL,
            json={"stocks": data},
            headers=headers,
            timeout=10
        )
        print(response)
        if response.status_code == 204:
            print(f"Успешно обновлено {len(data)} товаров")
            return True
        elif response.status_code == 409:
            print(f"Ошибка 409: Конфликт данных. Проверьте SKU: {[item['sku'] for item in data]}")
            return False
        else:
            print(f"Ошибка {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"Ошибка соединения: {str(e)}")
        return False

def main():
    df = get_supplier_data()
    df = process_availability(df)
    new_df = filter_by_brands(df)
    
    all_cards = get_all_cards(WB_API_KEY)
    stocks_data = prepare_products_data(df, all_cards)
    
    batch_size = 1000
    for i in range(0, len(stocks_data), batch_size):
        batch = stocks_data[i:i + batch_size]
        if send_data(batch):
            sleep(0.5)

if __name__ == "__main__":
    main()

