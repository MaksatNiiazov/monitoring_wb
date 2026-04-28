from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring", "0016_dailycampaignsearchclusterstat_order_sum"),
    ]

    operations = [
        migrations.CreateModel(
            name="WBApiRateLimit",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("scope", models.CharField(max_length=255, unique=True, verbose_name="WB API rate limit scope")),
                ("token_hash", models.CharField(blank=True, max_length=64, verbose_name="Token hash")),
                ("token_type", models.CharField(blank=True, max_length=32, verbose_name="Token type")),
                ("method", models.CharField(max_length=8, verbose_name="HTTP method")),
                ("base_url", models.CharField(max_length=255, verbose_name="Base URL")),
                ("path", models.CharField(max_length=255, verbose_name="Path")),
                ("next_request_at", models.DateTimeField(blank=True, null=True, verbose_name="Next request at")),
                ("last_status", models.PositiveSmallIntegerField(blank=True, null=True, verbose_name="Last status")),
                ("last_detail", models.TextField(blank=True, verbose_name="Last detail")),
                (
                    "last_headers",
                    models.JSONField(blank=True, default=dict, verbose_name="Last rate limit headers"),
                ),
            ],
            options={
                "verbose_name": "WB API rate limit",
                "verbose_name_plural": "WB API rate limits",
                "ordering": ["next_request_at", "scope"],
            },
        ),
    ]
