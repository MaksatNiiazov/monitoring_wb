from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("monitoring", "0007_dailycampaignsearchclusterstat"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProductKeyword",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="Создано")),
                ("updated_at", models.DateTimeField(auto_now=True, verbose_name="Обновлено")),
                ("query_text", models.CharField(max_length=255, verbose_name="Ключ")),
                ("position", models.PositiveSmallIntegerField(default=0, verbose_name="Позиция")),
                (
                    "product",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="keywords", to="monitoring.product"),
                ),
            ],
            options={
                "verbose_name": "Ключ товара",
                "verbose_name_plural": "Ключи товаров",
                "ordering": ["position", "query_text", "id"],
            },
        ),
        migrations.AddConstraint(
            model_name="productkeyword",
            constraint=models.UniqueConstraint(fields=("product", "query_text"), name="uniq_product_keyword"),
        ),
    ]
