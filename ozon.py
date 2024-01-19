import asyncio
import re
import time
from datetime import datetime as dt
from os import remove

import pandas as pd
from aiogram.types import CallbackQuery, FSInputFile
from bs4 import BeautifulSoup
from seleniumbase import Driver


def get_div_after_a_tag(a):
    try:
        return a.find_next_sibling("div")
    except Exception:
        return None


class OzonParser:
    def __init__(self, aiogram_call: CallbackQuery = None):
        self.key_word = ''
        self.key_words = []
        self.full_prices_list = {}
        self.discount_prices_list = {}
        self.discounts_list = {}
        self.products_list = []
        self.aiogram_call = aiogram_call
        self.driver = None

    async def get_html_of_the_page(self, url: str, do_not_scroll=False) -> str:
        if not self.driver:
            self.driver = Driver(uc=True, headless=True)
        self.driver.get(url)
        if not do_not_scroll:
            SCROLL_PAUSE_TIME = 2
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_PAUSE_TIME)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        page_html = str(self.driver.page_source)
        return page_html

    async def parse_amount_of_pages(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        try:
            s = list(map(lambda el: (int(el.getText()), 'https://ozon.ru' + el.get('href')), soup.find('div', class_='pe9').find('div', class_='eq0').find('div', class_='pe3').find('div', class_='p3e').find('div', class_='').find_all('a', class_='p1e')))
        except Exception:
            return {1: (True, html)}
        if len(s) > 1:
            numbers_and_links_dict = {1: (True, html)}
            for el in s[1:]:
                numbers_and_links_dict[el[0]] = (False, el[1])
            return numbers_and_links_dict
        else:
            return {1: (True, html)}

    async def parse_page_content(self, html: str) -> None:
        soup = BeautifulSoup(html, "html.parser")
        products_blocks = list(filter(lambda el: el, list(map(lambda el: get_div_after_a_tag(el.find('a')), soup.find('div', id='paginatorContent').find('div').find('div').find_all('div')))))
        counter = 0
        for product_block in products_blocks:
            try:
                full_name = product_block.find('a').find('span', class_='tsBody500Medium').getText().strip()
                link = 'https://ozon.ru' + product_block.find('a', class_='tile-hover-target').get('href')
                words = full_name.lower().replace('"', '').split()
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
                prices = list(map(lambda el: int(re.sub(r'[^\x00-\x7f]', '', el.text)), product_block.find('div').find('div').find_all('span')[:2]))
                full_price, discount_price = max(prices), min(prices)
                discount = full_price - discount_price
                if self.discount_prices_list:
                    middle_discount_price = int(sum(self.discount_prices_list) / len(self.discount_prices_list))
                else:
                    middle_discount_price = 0
                if middle_discount_price and discount_price + 0.51 * middle_discount_price < middle_discount_price:
                    continue
                product_data = {'Ссылка': link,
                                'Артикул': '-',
                                'Наименование': full_name,
                                'Продавец': '-',
                                'Цена без скидки': '-',
                                'Цена со скидкой': '-',
                                'Размер скидки': '-'}
                product_page_html = await self.get_html_of_the_page(url=link, do_not_scroll=True)
                soup = BeautifulSoup(product_page_html, "html.parser")
                try:
                    product_id = soup.find('span', attrs={'data-widget': 'webDetailSKU'}).get_text().replace(
                        'Код товара: ', '')
                except Exception:
                    product_id = None
                try:
                    product_seller = (soup.find('div', attrs={'data-widget': 'webCurrentSeller'})
                                      .find('a', attrs={'title': True}).get('title'))
                except Exception:
                    product_seller = None
                if product_id:
                    product_data['Артикул'] = product_id
                if product_seller:
                    product_data['Продавец'] = product_seller
                if full_price:
                    self.full_prices_list[full_price] = link
                    product_data['Цена без скидки'] = full_price
                if discount_price:
                    self.discount_prices_list[discount_price] = link
                    product_data['Цена со скидкой'] = discount_price
                if discount:
                    self.discounts_list[discount] = link
                    product_data['Размер скидки'] = discount
                self.products_list.append(product_data)
                counter += 1
                if self.aiogram_call:
                    await self.aiogram_call.message.edit_text(text=f"⚪️ Обработал товар №{counter} с артикулом: [{product_id}]({link})", parse_mode='markdown')
                else:
                    print(f"⚪️ Обработал товар №{counter} с артикулом: [{product_id}]({link})")
            except Exception:
                pass

    async def save_to_excel(self, file_name: str) -> str:
        data = pd.DataFrame(self.products_list)
        result_path = f"{file_name}.xlsx"
        writer = pd.ExcelWriter(result_path)
        data.to_excel(writer, 'data', index=False)
        writer.close()
        return result_path
    
    async def run_parser(self, key_word=None):
        if not key_word:
            self.key_word = input("Введите ключевые слова для поиска товара: ")
        else:
            self.key_word = key_word
        for word in self.key_word.lower().split():
            self.key_words += word.split('-')
        search_url = f'https://ozon.ru/search?text={self.key_word}'
        try:
            if self.aiogram_call:
                await self.aiogram_call.message.edit_text(text=f"🔗 Загружаю товары со страниц!")
            pages_info = await self.parse_amount_of_pages(await self.get_html_of_the_page(search_url))
        except Exception:
            print(f'Не удалось получить информацию о товаре {self.key_word} :(')
            return
        for page_num in pages_info:
            if not pages_info[page_num][0]:
                try:
                    pages_info[page_num] = (True, await self.get_html_of_the_page(pages_info[page_num][1]))
                except Exception:
                    pass
        if self.aiogram_call:
            await self.aiogram_call.message.edit_text(text=f"⚪️ Обрабатываю товары!")
        for page_num in pages_info:
            if pages_info[page_num][0]:
                page_html = pages_info[page_num][1]
                try:
                    await self.parse_page_content(page_html)
                except Exception:
                    pass
        self.driver.quit()
        self.driver = None

        table_path = await self.save_to_excel(f'{self.key_word}_{dt.now().strftime("%Y-%m-%d-%H-%M-%S")}')
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
                    caption=f'ℹ️ Информация о товаре *{self.key_word}*\n\n❌ [Максимальная скидочная цена (цена продажи): {max_discount_price}]({link_at_max_discount_price})\n🔶 Средняя скидочная цена (цена продажи): {middle_discount_price}\n✅ [Минимальная скидочная цена (цена продажи): {min_discount_price}]({link_at_min_discount_price})\n\n[🟥 Максимальная полная цена: {max_full_price}]({link_at_max_full_price})\n🟧 Средняя полная цена: {middle_full_price}\n[🟩 Минимальная полная цена: {min_full_price}]({link_at_min_full_price})\n\n[🟢 Максимальая скидка: {max_discount}]({link_at_max_discount})\n🟠 Средняя скидка: {middle_discount}\n[🔴 Минимальная скидка: {min_discount}]({link_at_min_discount})',
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
    parser = OzonParser()
    asyncio.run(parser.run_parser())
