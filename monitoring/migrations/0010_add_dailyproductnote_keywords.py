from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("monitoring", "0009_merge_20260403_2103"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailyproductnote",
            name="keywords",
            field=models.JSONField(blank=True, default=list, verbose_name="Ключи"),
        ),
    ]
