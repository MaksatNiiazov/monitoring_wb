from django.db import migrations, models


def clear_empty_keyword_rows(apps, schema_editor):
    DailyProductNote = apps.get_model("monitoring", "DailyProductNote")
    note_ids = []
    for note in DailyProductNote.objects.filter(keyword_rows_count__lte=3).only("id", "keywords", "keyword_rows_count"):
        if not note.keywords:
            note_ids.append(note.id)
    if note_ids:
        DailyProductNote.objects.filter(id__in=note_ids).update(keyword_rows_count=0)


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0019_dailyproductnote_action_fields"),
    ]

    operations = [
        migrations.AlterField(
            model_name="dailyproductnote",
            name="keyword_rows_count",
            field=models.PositiveSmallIntegerField(default=0, verbose_name="Строк ключей"),
        ),
        migrations.RunPython(clear_empty_keyword_rows, migrations.RunPython.noop),
    ]
