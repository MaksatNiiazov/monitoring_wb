from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0017_wbapiratelimit"),
    ]

    operations = [
        migrations.AlterField(
            model_name="campaign",
            name="monitoring_group",
            field=models.CharField(
                choices=[
                    ("unified", "Единая ставка"),
                    ("manual_search", "РС Поиск"),
                    ("manual_shelves", "РС Полки"),
                    ("manual_catalog", "РС Каталог"),
                    ("other", "Другое"),
                ],
                default="other",
                max_length=32,
                verbose_name="Группа мониторинга",
            ),
        ),
        migrations.AlterField(
            model_name="dailyproductnote",
            name="manual_search_enabled",
            field=models.BooleanField(default=False, verbose_name="Включили РК РС Поиск"),
        ),
        migrations.AlterField(
            model_name="dailyproductnote",
            name="manual_shelves_enabled",
            field=models.BooleanField(default=False, verbose_name="Включили РК РС Полки"),
        ),
    ]
