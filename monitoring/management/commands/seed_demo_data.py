from django.core.management.base import BaseCommand

from monitoring.services.demo import seed_demo_dataset


class Command(BaseCommand):
    help = "Заполняет проект демо-данными для презентации интерфейса."

    def handle(self, *args, **options):
        seed_demo_dataset()
        self.stdout.write(self.style.SUCCESS("Демо-данные загружены."))
