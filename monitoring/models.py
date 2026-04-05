from decimal import Decimal

from django.db import models


class CampaignMonitoringGroup(models.TextChoices):
    UNIFIED = "unified", "Единая ставка"
    MANUAL_SEARCH = "manual_search", "Руч. поиск"
    MANUAL_SHELVES = "manual_shelves", "Руч. полки"
    MANUAL_CATALOG = "manual_catalog", "Руч. каталог"
    OTHER = "other", "Другое"


class CampaignZone(models.TextChoices):
    SEARCH = "search", "Поиск"
    RECOMMENDATION = "recommendation", "Полки"
    CATALOG = "catalog", "Каталог"
    UNKNOWN = "unknown", "Неизвестно"


class SyncKind(models.TextChoices):
    FULL = "full", "Полная синхронизация"
    PRODUCT = "product", "Синхронизация товара"
    PRODUCT_META = "product_meta", "Обновление карточки товара"
    CAMPAIGN_META = "campaign_meta", "Обновление кампании"


class SyncStatus(models.TextChoices):
    RUNNING = "running", "В работе"
    SUCCESS = "success", "Успешно"
    ERROR = "error", "Ошибка"
    CANCELED = "canceled", "Отменено"


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Product(TimeStampedModel):
    nm_id = models.PositiveBigIntegerField(unique=True, verbose_name="Артикул WB")
    vendor_code = models.CharField(max_length=128, blank=True, verbose_name="Артикул продавца")
    title = models.CharField(max_length=255, blank=True, verbose_name="Название")
    brand_name = models.CharField(max_length=128, blank=True, verbose_name="Бренд")
    subject_name = models.CharField(max_length=128, blank=True, verbose_name="Предмет")
    buyout_percent = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=Decimal("24.00"),
        verbose_name="Процент выкупа, %",
    )
    unit_cost = models.DecimalField(max_digits=12, decimal_places=2, default=0, verbose_name="Себестоимость")
    logistics_cost = models.DecimalField(max_digits=12, decimal_places=2, default=0, verbose_name="Логистика")
    primary_keyword = models.CharField(max_length=255, blank=True, verbose_name="Ключ 1")
    secondary_keyword = models.CharField(max_length=255, blank=True, verbose_name="Ключ 2")
    is_active = models.BooleanField(default=True, verbose_name="Активен")

    class Meta:
        ordering = ["title", "nm_id"]
        verbose_name = "Товар"
        verbose_name_plural = "Товары"

    def __str__(self) -> str:
        return self.title or f"WB {self.nm_id}"

    def latest_economics(self):
        return self.economics_versions.order_by("-effective_from", "-id").first()

    def visible_warehouse_names(self) -> list[str]:
        return list(self.visible_warehouse_rules.order_by("warehouse_name").values_list("warehouse_name", flat=True))


class ProductKeyword(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="keywords")
    query_text = models.CharField(max_length=255, verbose_name="Ключ")
    position = models.PositiveSmallIntegerField(default=0, verbose_name="Позиция")

    class Meta:
        ordering = ["position", "query_text", "id"]
        constraints = [
            models.UniqueConstraint(fields=["product", "query_text"], name="uniq_product_keyword"),
        ]
        verbose_name = "Ключ товара"
        verbose_name_plural = "Ключи товаров"

    def __str__(self) -> str:
        return f"{self.product} / ключ / {self.query_text}"


class ProductEconomicsVersion(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="economics_versions")
    effective_from = models.DateField(verbose_name="Действует с")
    buyout_percent = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=Decimal("24.00"),
        verbose_name="Процент выкупа, %",
    )
    unit_cost = models.DecimalField(max_digits=12, decimal_places=2, default=0, verbose_name="Себестоимость")
    logistics_cost = models.DecimalField(max_digits=12, decimal_places=2, default=0, verbose_name="Логистика")

    class Meta:
        ordering = ["product_id", "-effective_from", "-id"]
        constraints = [
            models.UniqueConstraint(fields=["product", "effective_from"], name="uniq_product_economics_version"),
        ]
        verbose_name = "Версия экономики товара"
        verbose_name_plural = "Версии экономики товаров"

    def __str__(self) -> str:
        return f"{self.product} / экономика / {self.effective_from}"


class Campaign(TimeStampedModel):
    external_id = models.PositiveBigIntegerField(unique=True, verbose_name="ID РК")
    name = models.CharField(max_length=255, blank=True, verbose_name="Название кампании")
    bid_type = models.CharField(max_length=32, blank=True, verbose_name="Тип ставки")
    payment_type = models.CharField(max_length=32, blank=True, verbose_name="Тип оплаты")
    status = models.CharField(max_length=32, blank=True, verbose_name="Статус")
    monitoring_group = models.CharField(
        max_length=32,
        choices=CampaignMonitoringGroup.choices,
        default=CampaignMonitoringGroup.OTHER,
        verbose_name="Группа мониторинга",
    )
    placements = models.JSONField(default=dict, blank=True, verbose_name="Зоны показов")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")
    is_active = models.BooleanField(default=True, verbose_name="Активна")
    products = models.ManyToManyField(Product, through="ProductCampaign", related_name="campaigns", blank=True)

    class Meta:
        ordering = ["name", "external_id"]
        verbose_name = "Рекламная кампания"
        verbose_name_plural = "Рекламные кампании"

    def __str__(self) -> str:
        return self.name or f"РК {self.external_id}"


class ProductCampaign(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="product_campaigns")
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE, related_name="product_campaigns")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["product", "campaign"], name="uniq_product_campaign"),
        ]
        verbose_name = "Связь товара и РК"
        verbose_name_plural = "Связи товара и РК"

    def __str__(self) -> str:
        return f"{self.product} -> {self.campaign}"


class Warehouse(TimeStampedModel):
    office_id = models.PositiveBigIntegerField(unique=True, verbose_name="ID склада")
    name = models.CharField(max_length=255, verbose_name="Склад")
    region_name = models.CharField(max_length=255, blank=True, verbose_name="Регион")
    is_visible_in_monitoring = models.BooleanField(default=True, verbose_name="Показывать в мониторинге")

    class Meta:
        ordering = ["name", "office_id"]
        verbose_name = "Склад"
        verbose_name_plural = "Склады"

    def __str__(self) -> str:
        return self.name


class ProductVisibleWarehouse(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="visible_warehouse_rules")
    warehouse_name = models.CharField(max_length=255, verbose_name="Склад показа")

    class Meta:
        ordering = ["product_id", "warehouse_name"]
        constraints = [
            models.UniqueConstraint(fields=["product", "warehouse_name"], name="uniq_product_visible_warehouse"),
        ]
        verbose_name = "Склад показа для товара"
        verbose_name_plural = "Склады показа для товаров"

    def save(self, *args, **kwargs):
        self.warehouse_name = self.warehouse_name.strip()
        return super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.product} / {self.warehouse_name}"


class DailyProductMetrics(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="daily_metrics")
    stats_date = models.DateField(verbose_name="Дата статистики")
    open_count = models.PositiveIntegerField(default=0, verbose_name="Переходы в карточку")
    add_to_cart_count = models.PositiveIntegerField(default=0, verbose_name="Добавления в корзину")
    order_count = models.PositiveIntegerField(default=0, verbose_name="Заказы")
    order_sum = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Сумма заказов")
    buyout_count = models.PositiveIntegerField(default=0, verbose_name="Выкупы")
    buyout_sum = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Сумма выкупов")
    add_to_wishlist_count = models.PositiveIntegerField(default=0, verbose_name="Добавления в избранное")
    currency = models.CharField(max_length=16, default="RUB", verbose_name="Валюта")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")

    class Meta:
        ordering = ["-stats_date", "product_id"]
        constraints = [
            models.UniqueConstraint(fields=["product", "stats_date"], name="uniq_daily_product_metrics"),
        ]
        verbose_name = "Дневная воронка товара"
        verbose_name_plural = "Дневные воронки товаров"

    def __str__(self) -> str:
        return f"{self.product} / {self.stats_date}"


class DailyProductStock(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="daily_stocks")
    stats_date = models.DateField(verbose_name="Дата остатков")
    total_stock = models.PositiveIntegerField(default=0, verbose_name="Остатки WB")
    in_way_to_client = models.PositiveIntegerField(default=0, verbose_name="Едут к клиенту")
    in_way_from_client = models.PositiveIntegerField(default=0, verbose_name="Возвращаются на склад")
    avg_orders_per_day = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Среднее заказов/день")
    days_until_zero = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Дней до нуля")
    currency = models.CharField(max_length=16, default="RUB", verbose_name="Валюта")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")

    class Meta:
        ordering = ["-stats_date", "product_id"]
        constraints = [
            models.UniqueConstraint(fields=["product", "stats_date"], name="uniq_daily_product_stock"),
        ]
        verbose_name = "Дневные остатки товара"
        verbose_name_plural = "Дневные остатки товаров"

    def __str__(self) -> str:
        return f"{self.product} / остатки / {self.stats_date}"


class DailyWarehouseStock(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="warehouse_stocks")
    warehouse = models.ForeignKey(Warehouse, on_delete=models.CASCADE, related_name="product_stocks")
    stats_date = models.DateField(verbose_name="Дата остатков")
    stock_count = models.PositiveIntegerField(default=0, verbose_name="Остаток")
    in_way_to_client = models.PositiveIntegerField(default=0, verbose_name="Едут к клиенту")
    in_way_from_client = models.PositiveIntegerField(default=0, verbose_name="Возвращаются")
    avg_orders = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Средние заказы")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")

    class Meta:
        ordering = ["warehouse__name", "product_id"]
        constraints = [
            models.UniqueConstraint(
                fields=["product", "warehouse", "stats_date"],
                name="uniq_daily_warehouse_stock",
            ),
        ]
        verbose_name = "Дневные остатки по складу"
        verbose_name_plural = "Дневные остатки по складам"

    def __str__(self) -> str:
        return f"{self.product} / {self.warehouse} / {self.stats_date}"


class DailyCampaignProductStat(TimeStampedModel):
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE, related_name="daily_stats")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="campaign_stats")
    stats_date = models.DateField(verbose_name="Дата статистики")
    zone = models.CharField(
        max_length=32,
        choices=CampaignZone.choices,
        default=CampaignZone.UNKNOWN,
        verbose_name="Зона показов",
    )
    impressions = models.PositiveIntegerField(default=0, verbose_name="Показы")
    clicks = models.PositiveIntegerField(default=0, verbose_name="Клики")
    spend = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Расход")
    add_to_cart_count = models.PositiveIntegerField(default=0, verbose_name="Корзины")
    order_count = models.PositiveIntegerField(default=0, verbose_name="Заказы")
    units_ordered = models.PositiveIntegerField(default=0, verbose_name="ШК")
    order_sum = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Сумма заказов")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")

    class Meta:
        ordering = ["-stats_date", "campaign_id", "product_id", "zone"]
        constraints = [
            models.UniqueConstraint(
                fields=["campaign", "product", "stats_date", "zone"],
                name="uniq_campaign_product_zone_day",
            ),
        ]
        verbose_name = "Дневная статистика РК по товару"
        verbose_name_plural = "Дневные статистики РК по товарам"

    def __str__(self) -> str:
        return f"{self.campaign} / {self.product} / {self.zone} / {self.stats_date}"


class DailyCampaignSearchClusterStat(TimeStampedModel):
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE, related_name="daily_search_cluster_stats")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="search_cluster_stats")
    stats_date = models.DateField(verbose_name="Дата статистики")
    impressions = models.PositiveIntegerField(default=0, verbose_name="Показы")
    clicks = models.PositiveIntegerField(default=0, verbose_name="Клики")
    spend = models.DecimalField(max_digits=14, decimal_places=2, default=0, verbose_name="Расход")
    add_to_cart_count = models.PositiveIntegerField(default=0, verbose_name="Корзины")
    order_count = models.PositiveIntegerField(default=0, verbose_name="Заказы")
    units_ordered = models.PositiveIntegerField(default=0, verbose_name="ШК")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")

    class Meta:
        ordering = ["-stats_date", "campaign_id", "product_id"]
        constraints = [
            models.UniqueConstraint(
                fields=["campaign", "product", "stats_date"],
                name="uniq_campaign_product_search_cluster_day",
            ),
        ]
        verbose_name = "Дневная статистика search-кластеров РК по товару"
        verbose_name_plural = "Дневные статистики search-кластеров РК по товарам"

    def __str__(self) -> str:
        return f"{self.campaign} / {self.product} / clusters / {self.stats_date}"


class DailyProductKeywordStat(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="daily_keyword_stats")
    stats_date = models.DateField(verbose_name="Дата статистики")
    query_text = models.CharField(max_length=255, verbose_name="Поисковый запрос")
    frequency = models.PositiveIntegerField(default=0, verbose_name="Частота")
    organic_position = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Позиция органики")
    organic_orders = models.PositiveIntegerField(default=0, verbose_name="Заказы органики")
    boosted_position = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Позиция буста")
    boosted_ctr = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="CTR буста")
    boosted_views = models.PositiveIntegerField(default=0, verbose_name="Показы буста")
    boosted_clicks = models.PositiveIntegerField(default=0, verbose_name="Клики буста")
    raw_payload = models.JSONField(default=dict, blank=True, verbose_name="Сырой ответ API")

    class Meta:
        ordering = ["-stats_date", "product_id", "query_text"]
        constraints = [
            models.UniqueConstraint(
                fields=["product", "stats_date", "query_text"],
                name="uniq_daily_product_keyword_stat",
            ),
        ]
        verbose_name = "Дневная метрика ключа"
        verbose_name_plural = "Дневные метрики ключей"

    def __str__(self) -> str:
        return f"{self.product} / ключ / {self.query_text} / {self.stats_date}"


class DailyProductNote(TimeStampedModel):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="daily_notes")
    note_date = models.DateField(verbose_name="Дата заметки")
    spp_percent = models.DecimalField(max_digits=5, decimal_places=2, default=0, verbose_name="СПП, %")
    seller_price = models.DecimalField(max_digits=12, decimal_places=2, default=0, verbose_name="Цена WBSELLER")
    wb_price = models.DecimalField(max_digits=12, decimal_places=2, default=0, verbose_name="Цена WB")
    promo_status = models.CharField(max_length=255, blank=True, verbose_name="Акция")
    negative_feedback = models.CharField(max_length=255, blank=True, verbose_name="Негативные отзывы")
    unified_enabled = models.BooleanField(default=False, verbose_name="Включили РК единая ставка")
    manual_search_enabled = models.BooleanField(default=False, verbose_name="Включили РК руч. поиск")
    manual_shelves_enabled = models.BooleanField(default=False, verbose_name="Включили РК руч. полки")
    price_changed = models.BooleanField(default=False, verbose_name="Меняли цену")
    comment = models.TextField(blank=True, verbose_name="Комментарий")
    keywords = models.JSONField(default=list, blank=True, verbose_name="Ключи")
    keyword_rows_count = models.PositiveSmallIntegerField(default=3, verbose_name="Строк ключей")

    class Meta:
        ordering = ["-note_date", "product_id"]
        constraints = [
            models.UniqueConstraint(fields=["product", "note_date"], name="uniq_daily_product_note"),
        ]
        verbose_name = "Дневная заметка"
        verbose_name_plural = "Дневные заметки"

    def __str__(self) -> str:
        return f"{self.product} / заметка / {self.note_date}"


class SyncLog(TimeStampedModel):
    kind = models.CharField(max_length=32, choices=SyncKind.choices, default=SyncKind.FULL, verbose_name="Тип")
    status = models.CharField(max_length=16, choices=SyncStatus.choices, default=SyncStatus.RUNNING, verbose_name="Статус")
    target_date = models.DateField(null=True, blank=True, verbose_name="Целевая дата")
    finished_at = models.DateTimeField(null=True, blank=True, verbose_name="Завершено")
    message = models.TextField(blank=True, verbose_name="Сообщение")
    payload = models.JSONField(default=dict, blank=True, verbose_name="Детали")

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Лог синхронизации"
        verbose_name_plural = "Логи синхронизации"

    def __str__(self) -> str:
        return f"{self.get_kind_display()} / {self.get_status_display()} / {self.created_at:%d.%m.%Y %H:%M}"


class MonitoringSettings(TimeStampedModel):
    project_name = models.CharField(max_length=255, default="Мониторинг WB", verbose_name="Название проекта")
    report_timezone = models.CharField(max_length=64, default="Asia/Bishkek", verbose_name="Часовой пояс отчётов")
    sync_hour = models.PositiveSmallIntegerField(default=9, verbose_name="Час ежедневной синхронизации")
    sync_minute = models.PositiveSmallIntegerField(default=15, verbose_name="Минута ежедневной синхронизации")
    overwrite_within_day = models.BooleanField(default=True, verbose_name="Перезаписывать данные в рамках суток")
    monitoring_history_days = models.PositiveSmallIntegerField(default=14, verbose_name="Дней в мониторинге")
    visible_warehouses_note = models.TextField(blank=True, verbose_name="Комментарий по складам")
    campaign_grouping_note = models.TextField(blank=True, verbose_name="Комментарий по группировке РК")

    class Meta:
        verbose_name = "Настройки мониторинга"
        verbose_name_plural = "Настройки мониторинга"

    def save(self, *args, **kwargs):
        self.pk = 1
        return super().save(*args, **kwargs)

    @classmethod
    def get_solo(cls) -> "MonitoringSettings":
        return cls.objects.get_or_create(
            pk=1,
            defaults={
                "project_name": "Мониторинг WB",
                "report_timezone": "Asia/Bishkek",
                "sync_hour": 9,
                "sync_minute": 15,
                "overwrite_within_day": True,
                "monitoring_history_days": 14,
            },
        )[0]

    def __str__(self) -> str:
        return self.project_name
