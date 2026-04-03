import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())

def env_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def env_csv(name: str, default: str = "") -> list[str]:
    return [item.strip() for item in os.getenv(name, default).split(",") if item.strip()]

load_env_file(BASE_DIR / ".env")

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "django-insecure-wb-monitoring-dev-key")
DEBUG = env_bool("DJANGO_DEBUG", True)
ALLOWED_HOSTS = env_csv("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost,testserver")
CSRF_TRUSTED_ORIGINS = env_csv("DJANGO_CSRF_TRUSTED_ORIGINS", "")
CORS_ALLOWED_ORIGINS = env_csv("DJANGO_CORS_ALLOWED_ORIGINS", "")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "monitoring",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

if CORS_ALLOWED_ORIGINS:
    try:
        import corsheaders  # type: ignore  # noqa: F401
    except Exception:
        # CORS env is set, but optional dependency is not installed yet.
        CORS_ALLOWED_ORIGINS = []
    else:
        INSTALLED_APPS.insert(0, "corsheaders")
        MIDDLEWARE.insert(1, "corsheaders.middleware.CorsMiddleware")

ROOT_URLCONF = "wb_monitoring.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "monitoring.context_processors.workspace_settings",
            ],
        },
    },
]

WSGI_APPLICATION = "wb_monitoring.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "OPTIONS": {
            "timeout": int(os.getenv("SQLITE_TIMEOUT_SECONDS", "30")),
        },
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        "OPTIONS": {"min_length": 8},
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "ru-ru"
TIME_ZONE = os.getenv("DJANGO_TIME_ZONE", "Asia/Bishkek")
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

WB_ANALYTICS_API_TOKEN = os.getenv("WB_ANALYTICS_API_TOKEN", "")
WB_PROMOTION_API_TOKEN = os.getenv("WB_PROMOTION_API_TOKEN", "")
WB_REPORT_TIMEZONE = os.getenv("WB_REPORT_TIMEZONE", TIME_ZONE)
WB_STOCK_TYPE = os.getenv("WB_STOCK_TYPE", "wb")
WB_SYNC_HOUR = int(os.getenv("WB_SYNC_HOUR", "9"))
WB_SYNC_MINUTE = int(os.getenv("WB_SYNC_MINUTE", "15"))
WB_SYNC_SLEEP_SECONDS = int(os.getenv("WB_SYNC_SLEEP_SECONDS", "60"))
WB_APP_TYPE_ZONE_MAP = os.getenv("WB_APP_TYPE_ZONE_MAP", "1:recommendation,32:search,64:catalog")
