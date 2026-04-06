from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0014_monitoringsettings_table_default_modes"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="monitoringsettings",
            name="overwrite_within_day",
        ),
    ]
