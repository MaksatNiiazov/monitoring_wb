from django.db import migrations, models


def shrink_default_keyword_rows(apps, schema_editor):
    DailyProductNote = apps.get_model("monitoring", "DailyProductNote")
    DailyProductNote.objects.filter(keyword_rows_count=8).update(keyword_rows_count=3)


class Migration(migrations.Migration):
    dependencies = [
        ("monitoring", "0012_dailyproductnote_keyword_rows_count"),
    ]

    operations = [
        migrations.AlterField(
            model_name="dailyproductnote",
            name="keyword_rows_count",
            field=models.PositiveSmallIntegerField(default=3, verbose_name="Строк ключей"),
        ),
        migrations.RunPython(shrink_default_keyword_rows, migrations.RunPython.noop),
    ]
