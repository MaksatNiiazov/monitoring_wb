(function () {
    const RUNNING_POLL_MS = 2500;
    const HIDDEN_POLL_MULTIPLIER = 2;
    const DATE_TIME_FORMATTER = new Intl.DateTimeFormat("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    });

    function getCookie(name) {
        const cookieValue = document.cookie
            .split(";")
            .map((chunk) => chunk.trim())
            .find((chunk) => chunk.startsWith(`${name}=`));
        if (!cookieValue) {
            return "";
        }
        return decodeURIComponent(cookieValue.split("=").slice(1).join("="));
    }

    function formatDateTime(value) {
        if (!value) {
            return "-";
        }
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) {
            return "-";
        }
        return DATE_TIME_FORMATTER.format(parsed);
    }

    function chipClassByStatus(status) {
        if (status === "success") {
            return "success";
        }
        if (status === "error") {
            return "error";
        }
        if (status === "canceled") {
            return "neutral";
        }
        if (status === "running") {
            return "running";
        }
        return "neutral";
    }

    class SyncIndicator {
        constructor(root) {
            this.root = root;
            this.statusUrl = root.dataset.syncStatusUrl;
            this.cancelUrl = root.dataset.syncCancelUrl;
            this.titleNode = root.querySelector("[data-sync-title]");
            this.chipNode = root.querySelector("[data-sync-chip]");
            this.messageNode = root.querySelector("[data-sync-message]");
            this.progressWrapNode = root.querySelector("[data-sync-progress-wrap]");
            this.progressFillNode = root.querySelector("[data-sync-progress-fill]");
            this.progressPercentNode = root.querySelector("[data-sync-progress-percent]");
            this.progressStageNode = root.querySelector("[data-sync-progress-stage]");
            this.metaNode = root.querySelector("[data-sync-meta]");
            this.cancelButtons = Array.from(root.querySelectorAll("[data-sync-cancel]"));
            this.syncButtons = Array.from(document.querySelectorAll("[data-sync-submit]"));
            this.timer = null;
            this.cancelPending = false;
            this.isRunning = false;
            this.countdownTimer = null;
            this.retryUntil = null;

            this.syncButtons.forEach((button) => {
                if (!button.dataset.defaultLabel) {
                    button.dataset.defaultLabel = button.textContent.trim();
                }
            });

            this.cancelButtons.forEach((button) => {
                button.addEventListener("click", () => this.requestCancel());
            });

            document.addEventListener("visibilitychange", () => {
                if (!document.hidden && this.isRunning) {
                    this.poll();
                }
            });
            this.poll();
        }

        schedule(nextMs) {
            window.clearTimeout(this.timer);
            this.timer = window.setTimeout(() => this.poll(), nextMs);
        }

        startCountdown(retryUntil) {
            this.stopCountdown();
            this.retryUntil = retryUntil ? new Date(retryUntil).getTime() : null;
            if (!this.retryUntil || Number.isNaN(this.retryUntil)) {
                return;
            }

            const updateCountdown = () => {
                if (!this.retryUntil) return;
                const remaining = Math.max(0, Math.floor((this.retryUntil - Date.now()) / 1000));
                if (remaining <= 0) {
                    this.stopCountdown();
                    return;
                }
                // Обновляем текст сообщения если он содержит "Ждём" или "сек"
                if (this.messageNode && this.messageNode.textContent) {
                    const text = this.messageNode.textContent;
                    if (text.includes("Ждём") && text.includes("сек")) {
                        // Заменяем число секунд в тексте
                        this.messageNode.textContent = text.replace(/Ждём \d+ сек/, `Ждём ${remaining} сек`);
                    }
                }
            };

            updateCountdown(); // Сразу обновляем
            this.countdownTimer = window.setInterval(updateCountdown, 1000); // Каждую секунду
        }

        stopCountdown() {
            if (this.countdownTimer) {
                window.clearInterval(this.countdownTimer);
                this.countdownTimer = null;
            }
            this.retryUntil = null;
        }

        nextInterval() {
            return document.hidden ? RUNNING_POLL_MS * HIDDEN_POLL_MULTIPLIER : RUNNING_POLL_MS;
        }

        setIndicatorState(status) {
            this.root.classList.remove("is-running", "is-success", "is-error", "is-canceled");
            if (status === "running") {
                this.root.classList.add("is-running");
                return;
            }
            if (status === "success") {
                this.root.classList.add("is-success");
                return;
            }
            if (status === "error") {
                this.root.classList.add("is-error");
                return;
            }
            if (status === "canceled") {
                this.root.classList.add("is-canceled");
            }
        }

        setSyncButtonsDisabled(disabled) {
            this.syncButtons.forEach((button) => {
                button.disabled = Boolean(disabled);
                if (disabled) {
                    button.textContent = "Синхронизация выполняется...";
                } else if (button.dataset.defaultLabel) {
                    button.textContent = button.dataset.defaultLabel;
                }
            });
        }

        setCancelButtonsState({ isRunning, canCancel, cancelRequested }) {
            this.cancelButtons.forEach((button) => {
                button.hidden = !isRunning;
                if (!isRunning) {
                    button.disabled = false;
                    button.textContent = "Отменить синхронизацию";
                    return;
                }
                if (this.cancelPending) {
                    button.disabled = true;
                    button.textContent = "Отменяем...";
                    return;
                }
                if (cancelRequested) {
                    button.disabled = true;
                    button.textContent = "Отмена запрошена...";
                    return;
                }
                button.disabled = !canCancel;
                button.textContent = "Отменить синхронизацию";
            });
        }

        setChip(status, text) {
            if (!this.chipNode) {
                return;
            }
            this.chipNode.className = `status-chip ${chipClassByStatus(status)}`;
            this.chipNode.textContent = text;
        }

        setProgress(percent, stage) {
            if (!this.progressWrapNode || !this.progressFillNode || !this.progressPercentNode || !this.progressStageNode) {
                return;
            }
            const normalizedPercent = Math.max(0, Math.min(100, Number(percent || 0)));
            this.progressWrapNode.hidden = false;
            this.progressFillNode.style.width = `${normalizedPercent}%`;
            this.progressPercentNode.textContent = `${normalizedPercent}%`;
            this.progressStageNode.textContent = stage || "Ожидание";
        }

        hideProgress() {
            if (!this.progressWrapNode) {
                return;
            }
            this.progressWrapNode.hidden = true;
        }

        render(data) {
            if (!data || !data.has_sync) {
                this.isRunning = false;
                if (this.titleNode) {
                    this.titleNode.textContent = "Синхронизация ещё не запускалась";
                }
                this.setChip("idle", "Ожидание");
                this.setIndicatorState("idle");
                this.setSyncButtonsDisabled(false);
                this.setCancelButtonsState({ isRunning: false, canCancel: false, cancelRequested: false });
                if (this.messageNode) {
                    this.messageNode.textContent = "Запустите синхронизацию, чтобы увидеть прогресс обновления таблиц.";
                }
                this.hideProgress();
                if (this.metaNode) {
                    this.metaNode.textContent = "";
                }
                return;
            }

            const progress = data.progress || {};
            const stage = progress.stage || (data.is_running ? "Выполняется" : "Завершено");
            const detail = progress.detail || "";
            const message = data.message || detail || "";
            this.isRunning = Boolean(data.is_running);

            // Запускаем или останавливаем countdown для retry
            if (progress.retry_until && data.is_running) {
                this.startCountdown(progress.retry_until);
            } else {
                this.stopCountdown();
            }

            if (this.titleNode) {
                this.titleNode.textContent = `${data.kind_display}: ${data.status_display}`;
            }
            this.setChip(data.status, data.status_display || "Статус");
            this.setIndicatorState(data.status);
            this.setSyncButtonsDisabled(Boolean(data.is_running));
            this.setCancelButtonsState({
                isRunning: Boolean(data.is_running),
                canCancel: Boolean(data.can_cancel),
                cancelRequested: Boolean(data.cancel_requested),
            });
            if (this.messageNode) {
                this.messageNode.textContent = message || "Статус обновлён.";
            }

            if (data.is_running || Number(progress.percent || 0) > 0) {
                this.setProgress(progress.percent, stage);
            } else {
                this.hideProgress();
            }

            if (this.metaNode) {
                const startedAt = formatDateTime(data.created_at);
                const finishedAt = formatDateTime(data.finished_at);
                const updatedAt = formatDateTime(progress.updated_at);
                this.metaNode.textContent = data.is_running
                    ? `Запуск: ${startedAt}. Последнее обновление: ${updatedAt}.`
                    : `Запуск: ${startedAt}. Завершение: ${finishedAt}.`;
            }
        }

        async requestCancel() {
            if (!this.cancelUrl || this.cancelPending) {
                return;
            }
            this.cancelPending = true;
            this.setCancelButtonsState({ isRunning: true, canCancel: false, cancelRequested: true });
            try {
                const response = await fetch(this.cancelUrl, {
                    method: "POST",
                    headers: {
                        Accept: "application/json",
                        "X-Requested-With": "XMLHttpRequest",
                        "X-CSRFToken": getCookie("csrftoken"),
                    },
                    cache: "no-store",
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const payload = await response.json();
                if (this.messageNode && payload && payload.detail) {
                    this.messageNode.textContent = payload.detail;
                }
            } catch (_error) {
                if (this.messageNode) {
                    this.messageNode.textContent = "Не удалось отправить отмену. Попробуйте ещё раз.";
                }
            } finally {
                this.cancelPending = false;
                this.poll();
            }
        }

        async poll() {
            if (!this.statusUrl) {
                return;
            }
            // Не останавливаем countdown при poll, чтобы он продолжал работать между запросами
            try {
                const response = await fetch(this.statusUrl, {
                    method: "GET",
                    headers: {
                        Accept: "application/json",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                    cache: "no-store",
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const data = await response.json();
                this.render(data);
                if (data && data.is_running) {
                    this.schedule(this.nextInterval());
                } else {
                    window.clearTimeout(this.timer);
                    this.stopCountdown();
                }
            } catch (_error) {
                this.isRunning = false;
                this.stopCountdown();
                if (this.messageNode) {
                    this.messageNode.textContent = "Не удалось получить статус синхронизации. Обновите страницу или запустите синхронизацию повторно.";
                }
                this.setIndicatorState("idle");
                this.setSyncButtonsDisabled(false);
                this.setCancelButtonsState({ isRunning: false, canCancel: false, cancelRequested: false });
                window.clearTimeout(this.timer);
            }
        }
    }

    function initSyncIndicators() {
        document.querySelectorAll("[data-sync-indicator]").forEach((node) => {
            if (!node.dataset.syncBound) {
                node.dataset.syncBound = "1";
                new SyncIndicator(node);
            }
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initSyncIndicators);
    } else {
        initSyncIndicators();
    }
})();
