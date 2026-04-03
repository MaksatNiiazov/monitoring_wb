# Карта полей: какие данные берём из API

Дата актуализации: 22.03.2026

Документ описывает, откуда берётся каждое поле дневной матрицы мониторинга и какие значения считаются формулами/ручными.

## 1) Эндпоинты, которые использует sync

| Блок | Метод и endpoint | Где используется |
| --- | --- | --- |
| Воронка товара | `POST /api/analytics/v3/sales-funnel/products/history` | `DailyProductMetrics` |
| Остатки (товар) | `POST /api/v2/stocks-report/products/sizes` | `DailyProductStock`, `DailyWarehouseStock` |
| Остатки (метаданные товара) | `POST /api/v2/stocks-report/products/products` | обновление карточки `Product` |
| Рекламные кампании (метаданные) | `GET /api/advert/v2/adverts` | `Campaign`, привязки `ProductCampaign` |
| Рекламная статистика | `GET /adv/v3/fullstats` | `DailyCampaignProductStat` |
| Буст по ключам | `POST /adv/v1/normquery/stats` | `DailyProductKeywordStat` (boosted-часть) |
| Органика по ключам | `POST /api/v2/search-report/product/orders` | `DailyProductKeywordStat` (organic-часть) |
| Заказы поставщика | `GET /api/v1/supplier/orders` | `DailyProductNote` (`spp`, цены) |
| Цены (fallback) | `POST /api/v2/list/goods/filter` | `DailyProductNote` (если нет цен из supplier orders) |
| Отзывы | `GET /api/v1/feedbacks` | `DailyProductNote.negative_feedback` |

## 2) Маппинг по строкам дневного блока таблицы

| Поле в таблице | Источник в БД/коде | API path | Логика расчёта |
| --- | --- | --- | --- |
| Дата в заголовке блока | `report["stock_date"]` | не API | Берётся из выбранной даты среза (`reference_date`/история). |
| Доля трафика (%) | `DailyCampaignProductStat.impressions` | `fullstats.days[].apps[].nms[].views` | Для `Единая` считается доля внутри unified-группы. Для `Руч. Поиск/Полки` текущая логика ставит `100%`, если колонка активна. |
| Затраты (руб) | `DailyCampaignProductStat.spend` | `fullstats...sum` | По зонам из рекламы, `Общая` = `SUM(C:G)`. |
| Показы | `DailyCampaignProductStat.impressions` | `fullstats...views` | По зонам, `Общая` = `SUM(C:G)`. |
| CTR | формула по кликам/показам | `fullstats...clicks`, `fullstats...views` | `clicks / impressions * 100` по каждой зоне. |
| CPM | формула | `fullstats...sum`, `fullstats...views` | `spend * 1000 / impressions`. |
| CPC | формула | `fullstats...sum`, `fullstats...clicks` | `spend / clicks`. |
| Клики | `DailyCampaignProductStat.clicks` + `DailyProductMetrics.open_count` | `fullstats...clicks`, `sales-funnel...openCount` | `Общая` = из воронки, `ОРГ` = `Общая - SUM(реклама)`. |
| Корзины | `DailyCampaignProductStat.add_to_cart_count` + `DailyProductMetrics.add_to_cart_count` | `fullstats...atbs`, `sales-funnel...cartCount` | `ОРГ` = `Общая - SUM(реклама)`. |
| Заказы | `DailyCampaignProductStat.order_count` + `DailyProductMetrics.order_count` | `fullstats...orders`, `sales-funnel...orderCount` | `ОРГ` = `Общая - SUM(реклама)`. |
| Заказы (руб.) | `DailyCampaignProductStat.order_sum` + `DailyProductMetrics.order_sum` | `fullstats...sum_price`, `sales-funnel...orderSum` | `ОРГ` = `Общая - SUM(реклама)`. |
| Выкупы ≈ (руб.) | формула на базе экономики | см. выше + не API | `Заказы(руб) * Процент выкупа`. |
| Стоимость заказа | формула | см. выше | `Затраты / Заказы`. |
| Стоимость корзины | формула | см. выше | `Затраты / Корзины`. |
| ДРР от заказов (%) | формула | см. выше | `Затраты / (Заказы(руб)/100)`. |
| ДРР от продаж ≈ (%) | формула | см. выше + `buyout%` | `Затраты / (Выкупы≈/100)`. |
| Прибыль (без налогов...) | формула | смешанный источник | Формула от `seller_price`, `unit_cost`, `logistics`, `buyout%`, ДРР, заказов. |
| Процент выкупа % | `ProductEconomicsVersion.buyout_percent` | не API | Ручная экономика (версионируется внутри системы). |
| Себестоимость | `ProductEconomicsVersion.unit_cost` | не API | Ручная экономика. |
| Логистика | `ProductEconomicsVersion.logistics_cost` | не API | Ручная экономика. |
| Остатки на складах WB | `DailyProductStock.total_stock` | `sizes.data.sizes[].metrics.stockCount` | Сумма по всем размерам. |
| Едут к клиенту | `DailyProductStock.in_way_to_client` | `sizes...toClientCount` | Сумма по размерам. |
| Возвращаются на склад | `DailyProductStock.in_way_from_client` | `sizes...fromClientCount` | Сумма по размерам. |
| Ср. кол-во заказов/день | `DailyProductStock.avg_orders_per_day` | `sizes...avgOrders` | Сумма `avgOrders` по размерам/офисам. |
| Дней до распродажи в 0 | `DailyProductStock.days_until_zero` | производное | `total_stock / avg_orders_per_day`. |
| Ключи: Частота | `DailyProductKeywordStat.frequency` | `search-orders.data.items[].frequency` | Для `primary/secondary keyword`. |
| Ключи: поз. ОРГ | `DailyProductKeywordStat.organic_position` | `search-orders...dateItems[].avgPosition` | Берётся первая запись из `dateItems` на дату. |
| Ключи: поз. БУСТ | `DailyProductKeywordStat.boosted_position` | `normquery.items[].dailyStats[].stat.avgPos` | Взвешенное среднее по просмотрам. |
| Ключи: CTR (%) | `DailyProductKeywordStat.boosted_ctr` | `normquery...clicks/views` | `clicks / views * 100`. |
| Обзор: СПП | `DailyProductNote.spp_percent` | `supplier/orders[].spp` | Среднее значение по заказам за дату. |
| Обзор: Цена WBSELLER (наша) | `DailyProductNote.seller_price` | `supplier/orders[].priceWithDisc` | Мода по заказам; fallback из prices API (`discountedPrice`). |
| Обзор: Цена WB (на сайте) | `DailyProductNote.wb_price` | `supplier/orders[].finishedPrice` | Мода по заказам; fallback из prices API (`clubDiscountedPrice`). |
| Обзор: Акция | `DailyProductNote.promo_status` | не API | Поле ручное, по умолчанию `Не участвуем`. |
| Обзор: Негативные отзывы | `DailyProductNote.negative_feedback` | `feedbacks.data.feedbacks[].productValuation` | Автозаполнение: число отзывов с оценкой `<= 3`, иначе `Без изменений`. |
| Действия: включили РК/меняли цену/комментарии | `DailyProductNote.*` | не API | Полностью ручные поля пользователя. |

## 3) Важные замечания по расхождениям

| Ситуация | Почему может отличаться |
| --- | --- |
| Органика в кликах/корзинах/заказах отличается от эталона | В системе органика считается как `общая воронка - рекламная часть`, а не отдельным API источником органики. |
| Разница в процентах и абсолютных значениях | Часть строк в матрице вычисляется формулами (CTR/CPM/CPC/ДРР/прибыль), а не приходит напрямую из API. |
| Цена/СПП не совпадает с витриной WB | Приоритет источника: `supplier/orders`; если там нет значений, используется fallback из `prices` API. |
| Пустые рекламные колонки | По колонке нет фактической активности (`показы/клики/заказы/расход` = 0), поэтому ячейки очищаются. |

## 4) Что точно НЕ захардкожено

`DailyProductMetrics`, `DailyProductStock`, `DailyWarehouseStock`, `DailyCampaignProductStat`, `DailyProductKeywordStat` и авточасть `DailyProductNote` заполняются из ответов API в `monitoring/services/sync.py`.

Ручными остаются только поля заметок/действий (`promo_status`, чекбоксы действий, `comment`, часть текстового статуса), а также экономика товара (`buyout_percent`, `unit_cost`, `logistics_cost`).
