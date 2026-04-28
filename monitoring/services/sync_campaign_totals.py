"""
Альтернативная версия агрегации: суммирует ВСЕ товары кампании в зоне
для получения данных как в cmp.wildberries.ru
"""
from decimal import Decimal
from typing import Any
from monitoring.models import Product, Campaign, DailyCampaignProductStat


def aggregate_campaign_day_stats_with_totals(
    day_payload: dict[str, Any],
    *,
    product_map: dict[int, Product],
) -> dict[tuple[int, str], dict[str, Any]]:
    """
    Агрегирует статистику кампании за день.
    Для каждого продукта суммирует ВСЕ товары в зоне (как cmp.wildberries.ru).
    """
    from monitoring.services.sync import map_app_type_to_zone, decimalize
    
    aggregated: dict[tuple[int, str], dict[str, Any]] = {}
    zone_totals: dict[str, dict[str, Any]] = {}
    
    # Сначала суммируем ВСЕ товары по зонам
    for app_payload in day_payload.get("apps", []):
        app_type = app_payload.get("appType")
        zone = map_app_type_to_zone(app_type)
        
        if zone not in zone_totals:
            zone_totals[zone] = {
                "impressions": 0,
                "clicks": 0,
                "spend": Decimal("0"),
                "add_to_cart_count": 0,
                "order_count": 0,
                "units_ordered": 0,
                "order_sum": Decimal("0"),
                "raw_payload": [],
            }
        
        for item in app_payload.get("nms", []):
            zone_totals[zone]["impressions"] += int(item.get("views") or 0)
            zone_totals[zone]["clicks"] += int(item.get("clicks") or 0)
            zone_totals[zone]["spend"] += decimalize(item.get("sum"))
            zone_totals[zone]["add_to_cart_count"] += int(item.get("atbs") or 0)
            zone_totals[zone]["order_count"] += int(item.get("orders") or 0)
            zone_totals[zone]["units_ordered"] += int(item.get("shks") or 0)
            zone_totals[zone]["order_sum"] += decimalize(item.get("sum_price"))
            zone_totals[zone]["raw_payload"].append({"appType": app_type, "item": item})
    
    # Теперь присваиваем суммы зон каждому известному продукту
    for nm_id, product in product_map.items():
        for zone, totals in zone_totals.items():
            key = (product.id, zone)
            aggregated[key] = {
                "impressions": totals["impressions"],
                "clicks": totals["clicks"],
                "spend": totals["spend"],
                "add_to_cart_count": totals["add_to_cart_count"],
                "order_count": totals["order_count"],
                "units_ordered": totals["units_ordered"],
                "order_sum": totals["order_sum"],
                "raw_payload": totals["raw_payload"],
            }
    
    return aggregated


def get_campaign_totals_direct(
    campaign_id: int,
    stats_date,
    product: Product,
    client,
) -> dict[str, Any]:
    """
    Получает суммарные данные кампании по всем товарам (как cmp.wildberries.ru).
    Возвращает словарь с данными по зонам.
    """
    from monitoring.services.sync import map_app_type_to_zone, decimalize, quantize_money
    
    api_response = client.get_campaign_stats(
        ids=[campaign_id],
        start_date=stats_date,
        end_date=stats_date,
    )
    
    zone_map = {1: 'recommendation', 32: 'search', 64: 'catalog', 0: 'unknown'}
    result = {}
    
    for item in api_response:
        for day in item.get('days', []):
            for app in day.get('apps', []):
                app_type = app.get('appType')
                zone = zone_map.get(app_type, f'appType_{app_type}')
                
                if zone not in result:
                    result[zone] = {
                        'impressions': 0,
                        'clicks': 0,
                        'spend': Decimal('0'),
                        'add_to_cart_count': 0,
                        'order_count': 0,
                        'units_ordered': 0,
                        'order_sum': Decimal('0'),
                    }
                
                # Суммируем ВСЕ товары в зоне
                for nm in app.get('nms', []):
                    result[zone]['impressions'] += nm.get('views', 0)
                    result[zone]['clicks'] += nm.get('clicks', 0)
                    result[zone]['spend'] += decimalize(nm.get('sum'))
                    result[zone]['add_to_cart_count'] += nm.get('atbs', 0)
                    result[zone]['order_count'] += nm.get('orders', 0)
                    result[zone]['units_ordered'] += nm.get('shks', 0)
                    result[zone]['order_sum'] += decimalize(nm.get('sum_price'))
    
    # Конвертируем Decimal в float для удобства
    for zone in result:
        result[zone]['spend'] = float(quantize_money(result[zone]['spend']))
        result[zone]['order_sum'] = float(quantize_money(result[zone]['order_sum']))
    
    return result
