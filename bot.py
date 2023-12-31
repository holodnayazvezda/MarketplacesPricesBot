# импорты aiogram 
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook

# импорты других библиотек
import asyncio
import logging
import sys

# импорты из других файлов
from config import BOT_TOKEN, BUTTONS_TEXTS_AND_CALLBACK_DATAS
from wildberries import WildBerriesParser
from ozon import OzonParser
from yandexmarket import YandexMarketParser
from async_process_runner import start

dp = Dispatcher()


key_words = ''


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:  
    await message.answer(f'👋 Привет, *{message.from_user.full_name}*! Введите ключевые слова для получения статистики о цене продукта!\n\n_P.S. Как правило, двух слов почти всегда достаточно. Пример: "Витафон 2", "Витафон Т" или просто "Витафон". Регистр не учитывается._', parse_mode='markdown')


@dp.message()
async def handle_product_description(message: Message) -> None:
    global key_words
    key_words = message.text
    builder = InlineKeyboardBuilder()
    for button_text in BUTTONS_TEXTS_AND_CALLBACK_DATAS:
        builder.add(types.InlineKeyboardButton(text=button_text, callback_data=BUTTONS_TEXTS_AND_CALLBACK_DATAS[button_text]))
    await message.answer(f'Вы собираетесь получить статистику цена на товар: *"{message.text}"*. Выбери, в каком магазине искать!', parse_mode='markdown', reply_markup=builder.as_markup())


@dp.callback_query()
async def handle_shop(call: types.CallbackQuery) -> None:
    global key_words
    if call.data == 'wildberries':
        wb_parser = WildBerriesParser(call)
        await wb_parser.run_parser(key_words)
    elif call.data == 'ozon':
        ozon_parser = OzonParser(call)
        await ozon_parser.run_parser(key_words)
    elif call.data == 'yandex_market':
        ym_parser = YandexMarketParser(call)
        await ym_parser.run_parser(key_words)


async def main() -> None:
    bot = Bot(token=BOT_TOKEN)
    await bot(DeleteWebhook(drop_pending_updates=True))
    await dp.start_polling(bot)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
