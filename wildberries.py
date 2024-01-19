#!/usr/bin/env python3
"""Collect info on items from wildberries.ru and save it into an xlsx file.

This script is designed to extract data from the wildberries.ru website
and parsing all items in the search results based on a given keyword.

The script collects the following data from each item in the directory
or search results, which is then saved in xlsx format:
- Link
- ID
- Name
- Brand name
- Brand ID
- Regular price
- Discounted price
- Rating
- Number of reviews
- Total sales

---

Class: WildBerriesParser

Methods:
- __init__: Initialize the parser object.
- download_current_catalogue: Download the current catalogue in JSON format.
- traverse_json: Recursively traverse the JSON catalogue
    and flatten it to a list.
- process_catalogue: Process the locally saved JSON catalogue
    into a list of dictionaries.
- extract_category_data: Extract category data from the processed catalogue.
- get_products_on_page: Parse one page of category or search results
    and return a list with product data.
- add_data_from_page: Add data on products from a page to the class's list.
- get_all_products_in_category: Retrieve all products in a category
    by going through all pages.
- get_sales_data: Parse additional sales data for the product cards.
- save_to_excel: Save the parsed data in xlsx format and return its path.
- get_all_products_in_search_result: Retrieve all products in the search
    result by going through all pages.
- run_parser: Run the whole script for parsing and data processing.

---

Note: This script utilizes the requests library
and requires an active internet connection to function properly.

"""

import asyncio
from datetime import date
from datetime import datetime as dt
from os import path, remove

import pandas as pd
import requests
from aiogram.types import CallbackQuery, FSInputFile


class WildBerriesParser:
    def __init__(self, aiogram_call: CallbackQuery) -> None:
        self.headers = {'Accept': "*/*",
                        'User-Agent': "Chrome/51.0.2704.103 Safari/537.36"}
        self.run_date = date.today()
        self.product_cards = []
        self.directory = path.dirname(__file__)
        self.key_words = []
        self.full_prices_list = {}
        self.discount_prices_list = {}
        self.discounts_list = {}
        self.aiogram_call = aiogram_call

    def extract_category_data(self, catalogue: list, user_input: str) -> tuple:
        for category in catalogue:
            if (user_input.split("https://www.wildberries.ru")[-1]
                    == category['url'] or user_input == category['name']):
                return category['name'], category['shard'], category['query']

    def get_products_on_page(self, page_data: dict) -> list[dict]:
        products_on_page = []
        if not page_data:
            return
        for item in page_data['data']['products']:
            words = item['name'].lower().split()
            list_of_words = []
            for word in words:
                list_of_words += word.split('-')
            flag = True
            for key_word in self.key_words:
                if key_word not in list_of_words:
                    flag = False
                    break
            if not flag:
                continue
            full_price = int(item['priceU'] / 100)
            discount_price = int(item['salePriceU'] / 100)
            link = f"https://www.wildberries.ru/catalog/{item['id']}/detail.aspx"
            products_on_page.append({
                'Ссылка': link,
                'Артикул': item['id'],
                'Наименование': item['name'],
                'Продавец': item['supplier'],
                'Цена': full_price,
                'Цена со скидкой': discount_price,
                'Размер скидки': full_price - discount_price,
                'Рейтинг': item['rating'],
                'Отзывы': item['feedbacks']
            })
            if full_price:
                self.full_prices_list[full_price] = link
            if discount_price:
                self.discount_prices_list[discount_price] = link
            if full_price - discount_price:
                self.discounts_list[full_price - discount_price] = link
        return products_on_page

    def add_data_from_page(self, url: str):
        try:
            response = requests.get(url, headers=self.headers).json()
            page_data = self.get_products_on_page(response)
            if page_data and len(page_data) > 0:
                self.product_cards.extend(page_data)
            else:
                return True
        except Exception:
            return

    async def get_sales_data(self):
        if self.aiogram_call:
            await self.aiogram_call.message.edit_text(text=f"⚪️ Обрабатываю товары!")

        for card in self.product_cards:
            url = (f"https://product-order-qnt.wildberries.ru/by-nm/"
                   f"?nm={card['Артикул']}")
            try:
                response = requests.get(url, headers=self.headers).json()
                card['Продано'] = response[0]['qnt']
            except requests.ConnectTimeout:
                card['Продано'] = 'нет данных'
            if not self.aiogram_call:
                print(f"Обрабатываю товар: {self.product_cards.index(card) + 1} из {len(self.product_cards)}")

    def save_to_excel(self, file_name: str) -> str:
        data = pd.DataFrame(self.product_cards)
        result_path = (f"{path.join(self.directory, file_name)}_"
                       f"{self.run_date.strftime('%Y-%m-%d')}.xlsx")
        writer = pd.ExcelWriter(result_path)
        data.to_excel(writer, 'data', index=False)
        writer.close()
        return result_path

    async def get_all_products_in_search_result(self, key_word: str):
        if self.aiogram_call:
            await self.aiogram_call.message.edit_text(text=f"🔗 Загружаю товары со страниц!")

        for page in range(1, 10):
            if not self.aiogram_call:
                print(f"Загружаю товары со страницы {page}")
            url = (f"https://search.wb.ru/exactmatch/ru/common/v4/search?"
                   f"appType=1&curr=rub&dest=-1257786&page={page}"
                   f"&query={'%20'.join(key_word.split())}&resultset=catalog"
                   f"&sort=popular&spp=24&suppressSpellcheck=false")
            if self.add_data_from_page(url):
                break

    async def run_parser(self, key_word=''):
        if not key_word:
            key_word = input("Введите запрос для поиска: ")
        for word in key_word.lower().split():
            self.key_words += word.split('-')
        await self.get_all_products_in_search_result(key_word)
        try:
            await self.get_sales_data()
        except Exception:
            pass
        table_path = self.save_to_excel(key_word)
        table_aiogram = FSInputFile(path=table_path)
        time = dt.now().strftime("%Y-%m-%d %H:%M")
        if not self.aiogram_call:
            print(f'На момент времени: {time}')
        if len(self.discount_prices_list) > 1:
            max_discount_price, middle_discount_price, min_discount_price = max(self.discount_prices_list), int(sum(self.discount_prices_list) / len(self.discount_prices_list)), min(self.discount_prices_list)
            link_at_max_discount_price, link_at_min_discount_price = self.discount_prices_list[max_discount_price], self.discount_prices_list[min_discount_price]
            max_full_price, middle_full_price, min_full_price = max(self.full_prices_list), int(sum(self.full_prices_list) / len(self.full_prices_list)), min(self.full_prices_list)
            link_at_max_full_price, link_at_min_full_price = self.full_prices_list[max_full_price], self.full_prices_list[min_full_price]
            max_discount, middle_discount, min_discount = max(self.discounts_list), int(sum(self.discounts_list) / len(self.discounts_list)), min(self.discounts_list)
            link_at_max_discount, link_at_min_discount = self.discounts_list[max_discount], self.discounts_list[min_discount]
            if self.aiogram_call:
                await self.aiogram_call.message.answer_document(
                    document=table_aiogram,
                    caption=f'ℹ️ Информация о товаре *{key_word}*\n\n❌ [Максимальная скидочная цена (цена продажи): {max_discount_price}]({link_at_max_discount_price})\n🔶 Средняя скидочная цена (цена продажи): {middle_discount_price}\n✅ [Минимальная скидочная цена (цена продажи): {min_discount_price}]({link_at_min_discount_price})\n\n[🟥 Максимальная полная цена: {max_full_price}]({link_at_max_full_price})\n🟧 Средняя полная цена: {middle_full_price}\n[🟩 Минимальная полная цена: {min_full_price}]({link_at_min_full_price})\n\n[🟢 Максимальая скидка: {max_discount}]({link_at_max_discount})\n🟠 Средняя скидка: {middle_discount}\n[🔴 Минимальная скидка: {min_discount}]({link_at_min_discount})',
                    parse_mode='markdown'
                )
                await self.aiogram_call.message.delete()
            else:
                print(f'❌ Максимальная скидочная цена (цена продажи): {max_discount_price}') 
                print(f'🔶 Средняя скидочная цена (цена продажи): {middle_discount_price}')  
                print(f'✅ Минимальная скидочная цена (цена продажи): {min_discount_price}')
                print(f'🟥 Максимальная полная цена: {max_full_price}') 
                print(f'🟧 Средняя полная цена: {middle_full_price}')
                print(f'🟩 Минимальная полная цена: {min_full_price}')            
                print(f'🟢 Максимальая скидка: {max_discount}')
                print(f'🟠 Средняя скидка: {middle_discount}')
                print(f'🔴 Минимальная скидка: {min_discount}')
                print(f"Данные по каждому товару сохранены в {table_path} :)") 
        elif len(self.discount_prices_list) == 1:
            discount_price, full_price, discount = list(self.discount_prices_list.keys())[0], list(self.full_prices_list.keys())[0], list(self.discounts_list.keys())[0]
            link = self.discount_prices_list[discount_price]
            if self.aiogram_call:
                await self.aiogram_call.message.answer_document(
                    document=table_aiogram,
                    caption=f'В базе данных был сохранен только 1 [товар\n\n🟢 скидочная цена (цена продажи): {discount_price}\n🔴 Полная цена: {full_price}\n🟠 Cкидка: {discount}]({link})',
                    parse_mode='markdown'
                )
                await self.aiogram_call.message.delete()
            else:
                print('ℹ️ В базе данных был сохранен только 1 товар')
                print(f'🟢 Скидочная цена (цена продажи): {discount_price}')
                print(f'🔴 Полная цена: {full_price}')
                print(f'🟠 Cкидка: {discount}')
        else:
            if self.aiogram_call:
                await self.aiogram_call.message.answer(
                    text=f'Парсеру *не удалось* найти не одного товара, соответсвующего *данному* описанию :(',
                    parse_mode='markdown'
                )
                await self.aiogram_call.message.delete()
            else:
                print('ℹ️ Парсеру не удалось найти не одного товара, соответсвующего данному описанию :(')
        try:
            remove(table_path)
        except Exception:
            pass


if __name__ == '__main__':
    app = WildBerriesParser(None)
    asyncio.run(app.run_parser())