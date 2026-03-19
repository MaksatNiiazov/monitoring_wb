TEMPLATE_DEFINITIONS = {
    "products": {
        "filename": "wb_products_template.csv",
        "rows": [
            ["nm_id", "vendor_code", "title", "unit_cost", "logistics_cost", "buyout_percent", "primary_keyword", "secondary_keyword"],
            ["123456789", "SKU-001", "Женский спортивный костюм", "1500", "336", "24", "костюмы спортивные женский", "весенний женский костюм"],
        ],
    },
    "campaigns": {
        "filename": "wb_campaigns_template.csv",
        "rows": [
            ["external_id", "name", "monitoring_group", "product_nm_ids"],
            ["28150154", "Единая ставка", "unified", "123456789"],
            ["28150155", "Руч. поиск", "manual_search", "123456789"],
        ],
    },
    "warehouses": {
        "filename": "wb_warehouses_template.csv",
        "rows": [
            ["warehouse_name", "show_in_monitoring", "comment"],
            ["Коледино", "yes", "Основной склад"],
            ["Электросталь", "no", "Скрыть до запуска"],
        ],
    },
    "project_settings": {
        "filename": "wb_project_settings_template.csv",
        "rows": [
            ["parameter", "value"],
            ["report_timezone", "Asia/Bishkek"],
            ["daily_sync_time", "09:15"],
            ["overwrite_within_day", "yes"],
        ],
    },
}
