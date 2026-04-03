from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0006_dailyproductkeywordstat"),
    ]

    operations = [
        migrations.CreateModel(
            name="DailyCampaignSearchClusterStat",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("stats_date", models.DateField(verbose_name="Дата статистики")),
                ("impressions", models.PositiveIntegerField(default=0, verbose_name="Показы")),
                ("clicks", models.PositiveIntegerField(default=0, verbose_name="Клики")),
                ("spend", models.DecimalField(decimal_places=2, default=0, max_digits=14, verbose_name="Расход")),
                ("add_to_cart_count", models.PositiveIntegerField(default=0, verbose_name="Корзины")),
                ("order_count", models.PositiveIntegerField(default=0, verbose_name="Заказы")),
                ("units_ordered", models.PositiveIntegerField(default=0, verbose_name="ШК")),
                ("raw_payload", models.JSONField(blank=True, default=dict, verbose_name="Сырой ответ API")),
                (
                    "campaign",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="daily_search_cluster_stats",
                        to="monitoring.campaign",
                    ),
                ),
                (
                    "product",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="search_cluster_stats",
                        to="monitoring.product",
                    ),
                ),
            ],
            options={
                "verbose_name": "Дневная статистика search-кластеров РК по товару",
                "verbose_name_plural": "Дневные статистики search-кластеров РК по товарам",
                "ordering": ["-stats_date", "campaign_id", "product_id"],
            },
        ),
        migrations.AddConstraint(
            model_name="dailycampaignsearchclusterstat",
            constraint=models.UniqueConstraint(
                fields=("campaign", "product", "stats_date"),
                name="uniq_campaign_product_search_cluster_day",
            ),
        ),
    ]
