from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0018_alter_campaign_monitoring_group_labels"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailyproductnote",
            name="ads_budget",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                max_digits=12,
                verbose_name="Бюджет рекламы",
            ),
        ),
        migrations.AddField(
            model_name="dailyproductnote",
            name="price_change_amount",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                max_digits=12,
                verbose_name="Изменение цены, руб.",
            ),
        ),
        migrations.AddField(
            model_name="dailyproductnote",
            name="price_change_status",
            field=models.CharField(
                blank=True,
                default="",
                max_length=32,
                verbose_name="Изменение цены",
            ),
        ),
    ]
