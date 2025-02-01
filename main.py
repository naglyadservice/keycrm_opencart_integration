import time
import requests
import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector.errors import InternalError, OperationalError

load_dotenv()

def get_db_connection():
    """Создание подключения к базе данных"""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

def fetch_api_products() -> list[dict]:
    """Получение данных о продуктах с API"""
    url = os.getenv("API_URL_PRODUCTS")
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('API_KEY')}"
    }
    all_data = []
    page = 1

    while True:
        try:
            params = {"limit": 50, "page": page}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Вызывает исключение при ошибочных статус-кодах
            data = response.json()

            all_data.extend(data["data"])

            if not data.get("next_page_url"):
                break

            page += 1
            time.sleep(1)
            print(f"Обработка страницы {page} для продуктов...")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении данных о продуктах из API: {e}")
            return []

    return all_data

def fetch_api_offers() -> list[dict]:
    """Получение данных об офферах из API"""
    url = os.getenv("API_URL_OFFERS")
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('API_KEY')}"
    }
    all_data = []
    page = 1

    while True:
        try:
            params = {"limit": 50, "page": page}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            all_data.extend(data["data"])

            if not data.get("next_page_url"):
                break

            page += 1
            time.sleep(1)
            print(f"Обработка страницы {page} для офферов...")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при получении данных об офферах из API: {e}")
            return []

    return all_data

def update_product(cursor, model: str, quantity: int, price: float):
    """Обновление данных о продукте в базе данных"""
    try:
        cursor.execute(
            "UPDATE oc_product SET quantity = %s, price = %s WHERE model = %s",
            (quantity, price, model)
        )
    except Exception as e:
        print(f"Ошибка при обновлении продукта {model}: {e}")
        raise

def update_option_quantity(cursor, model: str, quantity: int):
    """Обновление количества в таблице опций"""
    try:
        cursor.execute(
            "UPDATE oc_product_option_value SET quantity = %s WHERE model = %s",
            (quantity, model)
        )
    except Exception as e:
        print(f"Ошибка при обновлении количества опции {model}: {e}")
        raise

def sync_products(cursor, api_products):
    """Синхронизация данных о продуктах"""
    for product in api_products:
        if not product.get("sku"):
            continue

        try:
            # Выполняем запрос и сразу получаем результаты
            cursor.execute(
                "SELECT product_id, model, quantity, price FROM oc_product WHERE model = %s",
                (product["sku"],)
            )
            db_product = cursor.fetchall()  # Получаем все результаты для очистки курсора

            if db_product:
                db_product = db_product[0]  # Берем первый результат
                product_price = product.get("price", product.get("max_price"))
                if (
                    db_product["quantity"] != product["quantity"] or
                    float(db_product["price"]) != float(product_price)
                ):
                    print(f"Обновление oc_product: SKU={db_product['model']}, количество={product['quantity']}, цена={product_price}")
                    update_product(
                        cursor=cursor,
                        model=db_product["model"],
                        quantity=product["quantity"],
                        price=product_price
                    )
                    cursor.fetchall()  # Очищаем результаты после обновления
                    time.sleep(1)
        except (InternalError, OperationalError) as e:
            print(f"Ошибка базы данных в sync_products: {e}")
            # Переподключение или обработка ошибки по необходимости
            raise

def sync_offers(cursor, api_offers):
    """Синхронизация данных об офферах"""
    updated_products = set()  # Множество для хранения артикулов продуктов, которые уже были обновлены

    for offer in api_offers:
        if not offer.get("sku"):
            continue

        try:
            # Извлекаем артикул продукта из артикула оффера (до дефиса)
            product_sku = offer["sku"].split("-")[0]

            # Если продукт уже был обновлен, пропускаем этот оффер
            if product_sku in updated_products:
                continue

            # Получаем данные о продукте из базы данных
            cursor.execute(
                "SELECT product_id, model, quantity, price FROM oc_product WHERE model = %s",
                (product_sku,)
            )
            db_product = cursor.fetchall()

            if db_product:
                db_product = db_product[0]  # Берем первый результат
                offer_price = offer.get("price", offer.get("max_price"))

                # Если цена оффера отличается от цены продукта, обновляем цену продукта
                if float(db_product["price"]) != float(offer_price):
                    print(f"Обновление oc_product: SKU={db_product['model']}, цена={offer_price}")
                    update_product(
                        cursor=cursor,
                        model=db_product["model"],
                        quantity=db_product["quantity"],  # Количество не меняем
                        price=offer_price
                    )
                    updated_products.add(product_sku)  # Добавляем артикул продукта в множество обновленных
                    cursor.fetchall()  # Очищаем результаты после обновления
                    time.sleep(1)

            # Обновляем количество в таблице опций (если нужно)
            cursor.execute(
                "SELECT model, quantity FROM oc_product_option_value WHERE model = %s",
                (offer["sku"],)
            )
            db_option = cursor.fetchall()

            if db_option:
                db_option = db_option[0]  # Берем первый результат
                if db_option["quantity"] != offer["quantity"]:
                    print(f"Обновление oc_product_option_value: Модель={db_option['model']}, количество={offer['quantity']}")
                    update_option_quantity(
                        cursor=cursor,
                        model=db_option["model"],
                        quantity=offer["quantity"]
                    )
                    cursor.fetchall()  # Очищаем результаты после обновления
                    time.sleep(0)
        except (InternalError, OperationalError) as e:
            print(f"Ошибка базы данных в sync_offers: {e}")
            # Переподключение или обработка ошибки по необходимости
            raise

def main():
    """Основная функция синхронизации"""
    try:
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
    except Exception as e:
        print(f"Ошибка в основной функции: {e}")
        conn.rollback()  # Откат изменений при ошибке
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main_loop():
    """Основной цикл программы"""
    while True:
        try:
            main()
            print("Ожидание 3600 секунд...")
            time.sleep(3600)  # Возвращено к 3600 секундам
        except Exception as e:
            print(f"Ошибка в основном цикле: {e}")
            time.sleep(60)  # Ожидание 60 секунд перед повторной попыткой при ошибке

if __name__ == "__main__":
    main_loop()