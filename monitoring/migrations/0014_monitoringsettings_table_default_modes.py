from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0013_adjust_dailyproductnote_keyword_rows_defaults"),
    ]

    operations = [
        migrations.AddField(
            model_name="monitoringsettings",
            name="table_default_compact_mode",
            field=models.BooleanField(default=True, verbose_name="Компактный режим таблицы по умолчанию"),
        ),
        migrations.AddField(
            model_name="monitoringsettings",
            name="table_default_fullscreen_mode",
            field=models.BooleanField(default=False, verbose_name="Полноэкранный режим таблицы по умолчанию"),
        ),
    ]
