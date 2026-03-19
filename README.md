# Мониторинг WB на Django

Проект автоматизирует заполнение мониторинга Wildberries: собирает данные из WB API, хранит историю в БД, показывает аналитику в веб-интерфейсе и формирует итоговую книгу мониторинга по шаблону из корня проекта.

## Что реализовано

- добавление товаров по `nmID` и рекламных кампаний по `ID РК`;
- хранение истории общей воронки, рекламы, остатков и складских срезов;
- разбиение рекламной статистики по зонам `Поиск`, `Полки`, `Каталог`;
- ручной и ежедневный запуск синхронизации;
- ручное обновление за выбранную дату с перезаписью данных;
- веб-интерфейс для управления, заметок и просмотра аналитики;
- формирование итоговой `.xlsx`-книги мониторинга:
  - один лист на товар;
  - история по датам;
  - блоки собраны по шаблону мониторинга;
  - расчётные ячейки выгружаются формулами;
- запись той же книги в Google Sheets;
- демо-режим и CSV-шаблоны для сбора данных от заказчика.

## Быстрый старт

```bash
python -m pip install -r requirements.txt
python manage.py migrate
python manage.py createsuperuser
python manage.py seed_demo_data
python manage.py runserver
```

После запуска откройте `http://127.0.0.1:8000/`.

## Переменные окружения

Скопируйте `.env.example` в `.env` и заполните:

- `WB_ANALYTICS_API_TOKEN` — доступ к аналитике и остаткам.
- `WB_PROMOTION_API_TOKEN` — доступ к рекламным кампаниям.
- `WB_APP_TYPE_ZONE_MAP` — маппинг `appType` в зоны мониторинга.
- `GOOGLE_SERVICE_ACCOUNT_FILE` или `GOOGLE_SERVICE_ACCOUNT_JSON` — доступ к Google Sheets.

`.env` подхватывается автоматически при старте проекта.

## Google Sheets

Чтобы запись в Google Sheets работала:

1. Создайте service account в Google Cloud.
2. Включите Google Sheets API.
3. Сохраните JSON-ключ.
4. Укажите путь в `GOOGLE_SERVICE_ACCOUNT_FILE` или вставьте содержимое в `GOOGLE_SERVICE_ACCOUNT_JSON`.
5. Выдайте доступ к целевой таблице для email service account.
6. На странице `/settings/` включите Google Sheets и заполните `ID Google таблицы`.

## Команды

```bash
python manage.py sync_wb_data
python manage.py sync_wb_data --reference-date 2026-03-17
python manage.py sync_wb_data --product-id 1
python manage.py export_monitoring_workbook --reference-date 2026-03-17 --history-days 14
python manage.py sync_google_sheets --reference-date 2026-03-17 --history-days 14
python manage.py seed_demo_data
python manage.py run_daily_sync_loop
```

`sync_wb_data` использует такую логику дат:

- рекламная статистика и общая воронка берутся за день до `reference-date`;
- остатки и складские срезы берутся за сам `reference-date`.

## Веб-интерфейс

- `/` — дашборд с аналитикой по товарам, товарами, РК и запуском синхронизации.
- `/settings/` — подготовка проекта, настройки, выгрузка книги мониторинга и запуск записи в Google Sheets.
- `/products/<id>/` — карточка товара с деталями, заметками и CSV-экспортом однодневного блока.

## Ограничения текущей версии

- автоматическая группировка кампаний по мониторинговым группам остаётся эвристической и может потребовать ручной корректировки;
- часть полей обзора (`СПП`, цены, комментарии, действия`) пока зависит от ежедневных заметок, если их нет в WB API;
- Google Sheets используется как внешний контур выгрузки, источник истины внутри проекта — БД.
