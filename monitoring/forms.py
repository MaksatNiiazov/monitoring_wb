from __future__ import annotations

from datetime import timedelta
import re
from types import SimpleNamespace

from django import forms
from django.db import transaction
from django.utils import timezone

from .models import (
    Campaign,
    DailyProductNote,
    MonitoringSettings,
    Product,
    ProductEconomicsVersion,
    ProductVisibleWarehouse,
    Warehouse,
)


def parse_warehouse_names(raw_value: str) -> list[str]:
    parts = [item.strip() for item in re.split(r"[\n,;]+", raw_value or "") if item.strip()]
    return list(dict.fromkeys(parts))


def latest_product_economics(product: Product):
    snapshot = product.latest_economics()
    if snapshot:
        return snapshot
    return SimpleNamespace(
        effective_from=timezone.localdate(),
        buyout_percent=product.buyout_percent,
        unit_cost=product.unit_cost,
        logistics_cost=product.logistics_cost,
    )


class StyledFormMixin:
    help_texts: dict[str, str] = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, field in self.fields.items():
            if name in self.help_texts:
                field.help_text = self.help_texts[name]

            widget = field.widget
            if isinstance(widget, forms.Textarea):
                widget.attrs.setdefault("rows", 4)
            if isinstance(widget, forms.NumberInput):
                widget.attrs.setdefault("inputmode", "decimal")
            if isinstance(widget, forms.DateInput):
                widget.attrs.setdefault("type", "date")
            if isinstance(widget, forms.SelectMultiple):
                widget.attrs.setdefault("size", 6)


class ProductCreateForm(StyledFormMixin, forms.ModelForm):
    help_texts = {
        "nm_id": "Артикул WB для синхронизации товара.",
        "buyout_percent": "Используется в расчётных метриках.",
        "unit_cost": "Себестоимость одной единицы.",
        "logistics_cost": "Логистика на единицу товара.",
        "primary_keyword": "Главный поисковый ориентир.",
        "secondary_keyword": "Дополнительный поисковый ориентир.",
    }

    class Meta:
        model = Product
        fields = [
            "nm_id",
            "buyout_percent",
            "unit_cost",
            "logistics_cost",
            "primary_keyword",
            "secondary_keyword",
        ]
        widgets = {
            "nm_id": forms.NumberInput(attrs={"placeholder": "Например, 123456789"}),
        }

    @transaction.atomic
    def save(self, commit: bool = True) -> Product:
        product = super().save(commit=False)
        if not commit:
            return product
        product.save()
        self.save_m2m()
        ProductEconomicsVersion.objects.update_or_create(
            product=product,
            effective_from=timezone.localdate(),
            defaults={
                "buyout_percent": product.buyout_percent,
                "unit_cost": product.unit_cost,
                "logistics_cost": product.logistics_cost,
            },
        )
        return product


class ProductSettingsForm(StyledFormMixin, forms.ModelForm):
    economics_effective_from = forms.DateField(
        initial=timezone.localdate,
        label="Экономика действует с",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    visible_warehouses = forms.MultipleChoiceField(
        required=False,
        label="Склады показа",
        choices=(),
        widget=forms.CheckboxSelectMultiple,
    )
    visible_warehouse_names_extra = forms.CharField(
        required=False,
        label="Дополнить вручную",
        widget=forms.Textarea(attrs={"rows": 2}),
    )
    help_texts = {
        "buyout_percent": "Участвует в прогнозах и расчётной прибыли.",
        "unit_cost": "Себестоимость единицы товара.",
        "logistics_cost": "Стоимость логистики на единицу товара.",
        "primary_keyword": "Главный ориентир для проверки поиска.",
        "secondary_keyword": "Дополнительный запрос для сравнения.",
        "is_active": "Неактивные товары остаются в истории, но не обновляются ежедневно.",
        "economics_effective_from": "Новая экономика начнёт влиять на эту дату и последующие.",
        "visible_warehouses": "Отметьте склады, которые должны участвовать в мониторинге по этому товару.",
        "visible_warehouse_names_extra": "Если склад ещё не попал в базу после sync, его можно дописать вручную.",
    }

    class Meta:
        model = Product
        fields = [
            "title",
            "vendor_code",
            "brand_name",
            "subject_name",
            "buyout_percent",
            "unit_cost",
            "logistics_cost",
            "primary_keyword",
            "secondary_keyword",
            "is_active",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.order_fields(
            [
                "title",
                "vendor_code",
                "brand_name",
                "subject_name",
                "buyout_percent",
                "unit_cost",
                "logistics_cost",
                "economics_effective_from",
                "primary_keyword",
                "secondary_keyword",
                "visible_warehouses",
                "visible_warehouse_names_extra",
                "is_active",
            ]
        )
        if self.instance and self.instance.pk:
            economics = latest_product_economics(self.instance)
            self.fields["buyout_percent"].initial = economics.buyout_percent
            self.fields["unit_cost"].initial = economics.unit_cost
            self.fields["logistics_cost"].initial = economics.logistics_cost
            selected_names = self.instance.visible_warehouse_names()
            discovered_names = sorted(
                set(selected_names)
                | set(
                    Warehouse.objects.filter(product_stocks__product=self.instance)
                    .order_by("name")
                    .values_list("name", flat=True)
                    .distinct()
                )
            )
            self.fields["visible_warehouses"].choices = [(name, name) for name in discovered_names]
            self.fields["visible_warehouses"].initial = [name for name in selected_names if name in discovered_names]
            self.fields["visible_warehouse_names_extra"].initial = ", ".join(
                [name for name in selected_names if name not in discovered_names]
            )
            if not discovered_names:
                self.fields["visible_warehouses"].help_text = (
                    "После первой синхронизации здесь появятся найденные склады. Пока можно использовать поле ниже."
                )
        self.fields["economics_effective_from"].initial = timezone.localdate()

    @transaction.atomic
    def save(self, commit: bool = True) -> Product:
        product = super().save(commit=False)
        if not commit:
            return product

        product.save()
        self.save_m2m()

        ProductEconomicsVersion.objects.update_or_create(
            product=product,
            effective_from=self.cleaned_data["economics_effective_from"],
            defaults={
                "buyout_percent": self.cleaned_data["buyout_percent"],
                "unit_cost": self.cleaned_data["unit_cost"],
                "logistics_cost": self.cleaned_data["logistics_cost"],
            },
        )

        ProductVisibleWarehouse.objects.filter(product=product).delete()
        selected_names = list(self.cleaned_data["visible_warehouses"])
        extra_names = parse_warehouse_names(self.cleaned_data["visible_warehouse_names_extra"])
        warehouse_rules = [
            ProductVisibleWarehouse(product=product, warehouse_name=name)
            for name in dict.fromkeys(selected_names + extra_names)
        ]
        if warehouse_rules:
            ProductVisibleWarehouse.objects.bulk_create(warehouse_rules)
        return product


class CampaignCreateForm(StyledFormMixin, forms.ModelForm):
    products = forms.ModelMultipleChoiceField(
        queryset=Product.objects.filter(is_active=True).order_by("title", "nm_id"),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 6}),
        label="Товары в мониторинге",
        help_text="Можно оставить пустым и привязать кампанию позже.",
    )
    help_texts = {
        "external_id": "ID рекламной кампании из кабинета WB.",
        "monitoring_group": "Определяет блок книги мониторинга.",
    }

    class Meta:
        model = Campaign
        fields = ["external_id", "monitoring_group", "products"]
        widgets = {
            "external_id": forms.NumberInput(attrs={"placeholder": "Например, 28150154"}),
        }


class DailyNoteForm(StyledFormMixin, forms.ModelForm):
    note_date = forms.DateField(widget=forms.HiddenInput())
    help_texts = {
        "spp_percent": "Если СПП не подтянулся автоматически, его можно указать вручную.",
        "seller_price": "Цена продавца на выбранную дату.",
        "wb_price": "Цена в витрине WB на выбранную дату.",
        "promo_status": "Короткая пометка по акции.",
        "negative_feedback": "Негатив за день: число или краткий комментарий.",
        "comment": "Гипотезы, отклонения и ручные действия за день.",
    }

    class Meta:
        model = DailyProductNote
        fields = [
            "note_date",
            "spp_percent",
            "seller_price",
            "wb_price",
            "promo_status",
            "negative_feedback",
            "unified_enabled",
            "manual_search_enabled",
            "manual_shelves_enabled",
            "price_changed",
            "comment",
        ]
        widgets = {
            "comment": forms.Textarea(attrs={"rows": 4}),
        }


class SyncForm(StyledFormMixin, forms.Form):
    help_texts = {
        "product_ids": "Если не выбрать — обновятся все активные товары.",
        "date_from": "Начало периода: с этой даты будут перезаписываться данные.",
        "date_to": "Конец периода: обновятся только записи внутри выбранного диапазона.",
        "reference_date": "Остатки, реклама и воронка будут собраны на эту же дату.",
        "force": "Перезапишет уже сохранённые данные только за выбранный период.",
    }

    product_ids = forms.ModelMultipleChoiceField(
        queryset=Product.objects.filter(is_active=True).order_by("title", "nm_id"),
        required=False,
        label="Товары для синхронизации",
        widget=forms.SelectMultiple(attrs={"size": 6, "class": "sync-product-select", "data-sync-product-select": "1"}),
    )
    reference_date = forms.DateField(
        required=False,
        initial=timezone.localdate,
        label="Дата запуска",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    force = forms.BooleanField(required=False, initial=True, label="Перезаписывать данные за дату")


    date_from = forms.DateField(
        required=False,
        initial=timezone.localdate,
        label="Дата с",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    date_to = forms.DateField(
        required=False,
        initial=timezone.localdate,
        label="Дата по",
        widget=forms.DateInput(attrs={"type": "date"}),
    )

    def __init__(self, *args, show_products: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["reference_date"].widget = forms.HiddenInput()
        if not show_products:
            self.fields["product_ids"].widget = forms.HiddenInput()
        self.order_fields(["product_ids", "date_from", "date_to", "force", "reference_date"])

    def clean(self):
        cleaned_data = super().clean()
        date_from = cleaned_data.get("date_from")
        date_to = cleaned_data.get("date_to")
        reference_date = cleaned_data.get("reference_date") or timezone.localdate()

        if not date_from and not date_to:
            date_from = reference_date
            date_to = reference_date
        elif date_from and not date_to:
            date_to = date_from
        elif date_to and not date_from:
            date_from = date_to

        if date_from and date_to and date_from > date_to:
            self.add_error("date_to", "Дата окончания должна быть не раньше даты начала.")

        cleaned_data["date_from"] = date_from
        cleaned_data["date_to"] = date_to
        # Для обратной совместимости со старым single-day режимом.
        cleaned_data["reference_date"] = date_to or reference_date
        return cleaned_data


class MonitoringWorkbookForm(StyledFormMixin, forms.Form):
    help_texts = {
        "reference_date": "Дата, относительно которой строится книга мониторинга.",
        "history_days": "Сколько дней включать в итоговую книгу.",
    }

    reference_date = forms.DateField(
        required=False,
        initial=timezone.localdate,
        label="Дата среза",
        widget=forms.DateInput(
            attrs={
                "type": "date",
                "autocomplete": "off",
            }
        ),
    )
    history_days = forms.IntegerField(
        min_value=1,
        max_value=90,
        initial=14,
        label="Дней в книге",
        widget=forms.NumberInput(
            attrs={
                "min": 1,
                "max": 90,
                "step": 1,
                "inputmode": "numeric",
            }
        ),
    )


class ReportsFilterForm(StyledFormMixin, forms.Form):
    RANGE_CHOICES = (
        (7, "7 дней"),
        (14, "14 дней"),
        (30, "30 дней"),
        (60, "60 дней"),
    )

    help_texts = {
        "date_from": "Начало периода аналитики (включительно).",
        "date_to": "Конец периода аналитики (включительно).",
        "range_days": "На такую глубину строятся графики и сравнительные отчёты.",
    }

    date_from = forms.DateField(
        required=False,
        label="Период с",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    date_to = forms.DateField(
        required=False,
        label="Период по",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    range_days = forms.TypedChoiceField(
        required=False,
        label="Окно аналитики",
        choices=RANGE_CHOICES,
        initial=14,
        coerce=int,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.order_fields(["date_from", "date_to", "range_days"])

    def clean(self):
        cleaned_data = super().clean()
        reference_date = timezone.localdate()
        range_days = cleaned_data.get("range_days") or 14
        date_from = cleaned_data.get("date_from")
        date_to = cleaned_data.get("date_to")

        normalized_window = max(1, min(int(range_days), 60))
        if date_from and not date_to:
            date_to = date_from + timedelta(days=normalized_window - 1)
        elif date_to and not date_from:
            date_from = date_to - timedelta(days=normalized_window - 1)

        if date_from and date_to:
            if date_from > date_to:
                self.add_error("date_to", "Дата окончания периода должна быть не раньше даты начала.")
                return cleaned_data
            selected_days = (date_to - date_from).days + 1
            if selected_days > 60:
                self.add_error("date_to", "Период аналитики не должен превышать 60 дней.")
                return cleaned_data
            reference_date = date_to
            range_days = selected_days
        else:
            range_days = normalized_window
            date_to = reference_date
            date_from = reference_date - timedelta(days=range_days - 1)

        cleaned_data["date_from"] = date_from
        cleaned_data["date_to"] = date_to
        # Сохраняем вычисленную опорную дату для обратной совместимости
        # с существующим кодом контекста и шаблонов.
        reference_date = date_to or reference_date
        cleaned_data["reference_date"] = reference_date
        cleaned_data["range_days"] = range_days
        return cleaned_data


class MonitoringSettingsForm(StyledFormMixin, forms.ModelForm):
    help_texts = {
        "project_name": "Название в интерфейсе и в книге.",
        "report_timezone": "Например, Asia/Bishkek или Europe/Moscow.",
        "sync_hour": "Час автосинхронизации.",
        "sync_minute": "Минуты автосинхронизации.",
        "overwrite_within_day": "Повторный запуск в тот же день обновит данные за эту дату.",
        "monitoring_history_days": "Глубина истории для книги и витрины.",
        "visible_warehouses_note": "Какие склады должны отображаться в мониторинге.",
        "campaign_grouping_note": "Как кампании раскладываются по группам мониторинга.",
    }

    class Meta:
        model = MonitoringSettings
        fields = [
            "project_name",
            "report_timezone",
            "sync_hour",
            "sync_minute",
            "overwrite_within_day",
            "monitoring_history_days",
            "visible_warehouses_note",
            "campaign_grouping_note",
        ]
        widgets = {
            "project_name": forms.TextInput(attrs={"placeholder": "Например, MB Bags / WB Monitoring"}),
            "report_timezone": forms.TextInput(attrs={"placeholder": "Asia/Bishkek"}),
            "visible_warehouses_note": forms.Textarea(attrs={"rows": 3}),
            "campaign_grouping_note": forms.Textarea(attrs={"rows": 3}),
        }

