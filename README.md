# MarketplacesPricesBot

Telegram-бот для поиска товаров по ключевым словам и получения статистики цен на Ozon, Wildberries и Яндекс Маркете. Парсеры используют HTTP-запросы или SeleniumBase, результаты обрабатываются через pandas и выгружаются в таблицы.

## Запуск

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python bot.py
```

Перед запуском укажите собственный `BOT_TOKEN` в `config.py`. Для браузерных парсеров подготовьте окружение SeleniumBase с браузером Chrome/Chromium.

В Telegram отправьте `/start`, введите название товара и выберите магазин. Реализации находятся в `ozon.py`, `wildberries.py` и `yandexmarket.py`. Получение данных зависит от доступности сайтов и их текущей HTML-разметки.
