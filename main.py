import time
import requests
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )


def fetch_api_products() -> list[dict]:
    """
    Получение данных о продуктах с основного эндпоинта.
    """
    url = os.getenv("API_URL_PRODUCTS")
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('API_KEY')}"
    }
    all_data = []
    page = 1

    while True:
        params = {"limit": 50, "page": page}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        all_data.extend(data["data"])

        if not data.get("next_page_url"):
            break

        page += 1
        time.sleep(1)
        print(f"Processing page {page} for products...")

    return all_data


def fetch_api_offers() -> list[dict]:
    """
    Получение данных об офферах с эндпоинта /offers.
    """
    url = os.getenv("API_URL_OFFERS")
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('API_KEY')}"
    }
    all_data = []
    page = 1

    while True:
        params = {"limit": 50, "page": page}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        all_data.extend(data["data"])

        if not data.get("next_page_url"):
            break

        page += 1
        time.sleep(1)
        print(f"Processing page {page} for offers...")

    return all_data

def sync_products(cursor, api_products):
    """
    Синхронизация таблицы oc_product с данными API.
    """
    for product in api_products:
        if not product.get("sku"):
            continue

        cursor.execute(
            "SELECT product_id, model, quantity, price FROM oc_product WHERE model = %s",
            (product["sku"],)
        )
        db_product = cursor.fetchone()

        if db_product:
            product_price = product.get("price", product.get("max_price"))
            if (
                db_product["quantity"] != product["quantity"] or
                float(db_product["price"]) != float(product_price)
            ):
                print(f"Updating oc_product: SKU={db_product['model']}, quantity={product['quantity']}, price={product_price}")
                #update_product(
                #    cursor=cursor,
                #    model=db_product["sku"],
                #    quantity=product["quantity"],
                #    price=product_price
                #)


def sync_offers(cursor, api_offers):
    """
    Синхронизация таблицы oc_product_option_value с данными API.
    Используется столбец model вместо sku.
    """
    for offer in api_offers:
        if not offer.get("sku"):
            continue

        cursor.execute(
            "SELECT model, quantity FROM oc_product_option_value WHERE model = %s",
            (offer["sku"],)
        )
        db_option = cursor.fetchone()

        if db_option:
            if db_option["quantity"] != offer["quantity"]:
                print(f"Updating oc_product_option_value: Model={db_option['model']}, quantity={offer['quantity']}")
                #update_option_quantity(
                #    cursor=cursor,
                #    model=db_option["model"],
                #    quantity=offer["quantity"]
                #)


def main():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Получение данных из API
    api_products = fetch_api_products()
    api_offers = fetch_api_offers()

    # Синхронизация данных
    sync_products(cursor, api_products)
    sync_offers(cursor, api_offers)

    # Применение изменений
    conn.commit()
    cursor.close()
    conn.close()


def main_loop():
    while True:
        main()
        print("Sleeping for 3600 seconds...")
        time.sleep(60)  # Интервал в 3600 секунд

if __name__ == "__main__":
    main_loop()