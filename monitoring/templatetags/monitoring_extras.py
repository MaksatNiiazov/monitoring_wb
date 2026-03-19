from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django import template

register = template.Library()


def _to_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


@register.filter
def money(value):
    number = _to_decimal(value)
    if number is None:
        return "-"
    return f"{number.quantize(Decimal('0.01')):,.2f}".replace(",", " ").replace(".", ",")


@register.filter
def percent(value):
    number = _to_decimal(value)
    if number is None:
        return "-"
    return f"{number.quantize(Decimal('0.01'))}%".replace(".", ",")


@register.filter
def intspace(value):
    number = _to_decimal(value)
    if number is None:
        return "-"
    return f"{int(number):,}".replace(",", " ")


@register.filter
def decimal2(value):
    number = _to_decimal(value)
    if number is None:
        return "-"
    return f"{number.quantize(Decimal('0.01'))}".replace(".", ",")


@register.filter
def widget_type(bound_field):
    widget = getattr(getattr(bound_field, "field", None), "widget", None)
    if widget is None:
        return ""
    return widget.__class__.__name__
