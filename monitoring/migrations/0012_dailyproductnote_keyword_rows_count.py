from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("monitoring", "0011_alter_dailyproductkeywordstat_options_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailyproductnote",
            name="keyword_rows_count",
            field=models.PositiveSmallIntegerField(default=8, verbose_name="Строк ключей"),
        ),
    ]
