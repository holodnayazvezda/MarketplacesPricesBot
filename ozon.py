from seleniumbase import Driver
from bs4 import BeautifulSoup
from aiogram.types import CallbackQuery

import time
import asyncio
import re
from datetime import datetime as dt


class OzonParser:
    def __init__(self, aiogram_call: CallbackQuery):
        self.key_word = ''
        self.key_words = []
        self.full_prices_list = {}
        self.discount_prices_list = {}
        self.discounts_list = {}
        self.aiogram_call = aiogram_call
        self.driver = None

    async def get_html_of_the_page(self, url: str) -> str:
        if not self.driver:
            self.driver = Driver(uc=True, headless=True, incognito=True)
        self.driver.get(url)
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
            self.driver.quit()
            self.driver = None
            return {1: (True, html)}
    
    async def parse_page_content(self, html: str) -> None:
        soup = BeautifulSoup(html, "html.parser")
        products_blocks = list(map(lambda el: el.find('div', class_='i7w'), soup.find('div', id='paginatorContent').find('div', class_='widget-search-result-container y8i').find('div', class_='iy9').find_all('div', class_='i6w iw7')))
        for product_block in products_blocks:
            full_name = product_block.find('a', class_='tile-hover-target i3t it4').find('div', class_='b8a ac ac0 i3t').find('span', class_='tsBody500Medium').getText().strip()
            link = 'https://ozon.ru' + product_block.find('a', class_='tile-hover-target i3t it4').get('href')
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
            prices = list(map(lambda el: int(re.sub(r'[^\x00-\x7f]', '', el.text)), product_block.find('div', class_='i3t').find('div', class_='c3124-a0').find_all('span')[:2]))
            full_price, discount_price = max(prices), min(prices)
            if self.discount_prices_list:
                middle_discount_price = int(sum(self.discount_prices_list) / len(self.discount_prices_list))
            else:
                middle_discount_price = 0
            if middle_discount_price and discount_price + 0.51 * middle_discount_price < middle_discount_price:
                continue
            if full_price:
                self.full_prices_list[full_price] = link
            if discount_price:
                self.discount_prices_list[discount_price] = link
            if full_price - discount_price:
                self.discounts_list[full_price - discount_price] = link
    
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
            print(f'Неудалось получить информацию о товаре {self.key_word} :(')
            return
        for page_num in pages_info:
            if not pages_info[page_num][0]:
                try:
                    pages_info[page_num] = (True, await self.get_html_of_the_page(pages_info[page_num][1]))
                except Exception:
                    pass
        self.driver.quit()
        self.driver = None
        if self.aiogram_call:
            await self.aiogram_call.message.edit_text(text=f"⚪️ Обрабатываю товары!")
        for page_num in pages_info:
            if pages_info[page_num][0]:
                page_html = pages_info[page_num][1]
                await self.parse_page_content(page_html)
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
                await self.aiogram_call.message.answer(
                    text=f'ℹ️ Информация о товаре *{self.key_word}*\n\n❌ [Максимальная скидочная цена (цена продажи): {max_discount_price}]({link_at_max_discount_price})\n🔶 Средняя скидочная цена (цена продажи): {middle_discount_price}\n✅ [Минимальная скидочная цена (цена продажи): {min_discount_price}]({link_at_min_discount_price})\n\n[🟥 Максимальная полная цена: {max_full_price}]({link_at_max_full_price})\n🟧 Средняя полная цена: {middle_full_price}\n[🟩 Минимальная полная цена: {min_full_price}]({link_at_min_full_price})\n\n[🟢 Максимальая скидка: {max_discount}]({link_at_max_discount})\n🟠 Средняя скидка: {middle_discount}\n[🔴 Минимальная скидка: {min_discount}]({link_at_min_discount})',
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
            if self.aiogram_call:
                await self.aiogram_call.message.answer(
                    text=f'В базе данных был сохранен только 1 товар\n\n🟢 скидочная цена (цена продажи): {self.discount_prices_list[0]}\n🔴 Полная цена: {self.full_prices_list[0]}\n🟠 Cкидка: {self.discounts_list[0]}',
                    parse_mode='markdown'
                )
                await self.aiogram_call.message.delete()
            else:
                print('ℹ️ В базе данных был сохранен только 1 товар')
                print(f'🟢 Скидочная цена (цена продажи): {self.discount_prices_list[0]}')
                print(f'🔴 Полная цена: {self.full_prices_list[0]}')
                print(f'🟠 Cкидка: {self.discounts_list[0]}')
        else:
            if self.aiogram_call:
                await self.aiogram_call.message.answer(
                    text=f'Парсеру *не удалось* найти не одного товара, соответсвующего *данному* описанию :(',
                    parse_mode='markdown'
                )
                await self.aiogram_call.message.delete()
            else:
                print('ℹ️ Парсеру не удалось найти не одного товара, соответсвующего данному описанию :(')


if __name__ == '__main__':
    parser = OzonParser(None)
    asyncio.run(parser.run_parser())
