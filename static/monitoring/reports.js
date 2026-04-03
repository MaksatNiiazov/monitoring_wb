(function () {
    const REPORTS_TAB_STORAGE_KEY = "wb-reports-active-tab";
    const INT_FORMATTER = new Intl.NumberFormat("ru-RU");
    const MONEY_FORMATTER = new Intl.NumberFormat("ru-RU", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
    const PERCENT_FORMATTER = new Intl.NumberFormat("ru-RU", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    });
    const DECIMAL_FORMATTER = new Intl.NumberFormat("ru-RU", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    });

    function formatValue(value, format) {
        const number = Number(value || 0);
        if (format === "money") {
            return `${MONEY_FORMATTER.format(number)} ₽`;
        }
        if (format === "percent") {
            return `${PERCENT_FORMATTER.format(number)}%`;
        }
        if (format === "decimal") {
            return DECIMAL_FORMATTER.format(number);
        }
        return INT_FORMATTER.format(Math.round(number));
    }

    function average(values) {
        if (!values.length) {
            return 0;
        }
        return values.reduce((sum, value) => sum + value, 0) / values.length;
    }

    function svgEl(tag, attrs = {}, children = "") {
        const attrString = Object.entries(attrs)
            .map(([key, value]) => `${key}="${String(value).replace(/"/g, "&quot;")}"`)
            .join(" ");
        return `<${tag}${attrString ? ` ${attrString}` : ""}>${children}</${tag}>`;
    }

    function pointFor(index, value, valuesCount, width, height, padding, maxValue) {
        const innerWidth = width - padding.left - padding.right;
        const innerHeight = height - padding.top - padding.bottom;
        const step = valuesCount > 1 ? innerWidth / (valuesCount - 1) : 0;
        const x = valuesCount > 1 ? padding.left + step * index : padding.left + innerWidth / 2;
        const normalized = maxValue > 0 ? value / maxValue : 0;
        const y = padding.top + innerHeight - innerHeight * normalized;
        return { x, y };
    }

    function linePath(points) {
        return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
    }

    function areaPath(points, baseline) {
        if (!points.length) {
            return "";
        }
        const start = `M ${points[0].x} ${baseline}`;
        const line = points.map((point) => `L ${point.x} ${point.y}`).join(" ");
        const end = `L ${points[points.length - 1].x} ${baseline} Z`;
        return `${start} ${line} ${end}`;
    }

    class ChartWidget {
        constructor(root) {
            this.root = root;
            this.stage = root.querySelector(".chart-stage");
            this.caption = root.querySelector(".chart-caption");
            this.legend = root.querySelector(".chart-legend");
            this.metricButtons = Array.from(root.querySelectorAll("[data-chart-metric]"));
            this.typeButtons = Array.from(root.querySelectorAll("[data-chart-type]"));

            const scriptId = root.dataset.chartScript;
            const scriptNode = document.getElementById(scriptId);
            this.data = scriptNode ? JSON.parse(scriptNode.textContent || "{}") : {};
            this.metric = this.data.defaultMetric || this.metricButtons[0]?.dataset.chartMetric || "orders";
            this.type = this.normalizeType(this.data.defaultType || this.typeButtons[0]?.dataset.chartType || "line");

            this.metricButtons.forEach((button) => {
                button.addEventListener("click", () => {
                    this.metric = button.dataset.chartMetric;
                    this.render();
                });
            });
            this.typeButtons.forEach((button) => {
                button.addEventListener("click", () => {
                    this.type = this.normalizeType(button.dataset.chartType);
                    this.render();
                });
            });

            if ("ResizeObserver" in window) {
                this.resizeObserver = new ResizeObserver(() => this.render());
                this.resizeObserver.observe(this.stage);
            } else {
                window.addEventListener("resize", () => this.render());
            }

            this.render();
        }

        normalizeType(nextType) {
            const normalized = String(nextType || "").toLowerCase();
            if (normalized === "bar" || normalized === "line") {
                return normalized;
            }
            return this.typeButtons[0]?.dataset.chartType || "line";
        }

        renderEmpty() {
            this.stage.innerHTML = '<div class="chart-empty">Данных пока недостаточно для построения графика.</div>';
            this.caption.textContent = "Сначала выполните синхронизацию и сформируйте срез за выбранный период.";
            this.legend.innerHTML = "";
            this.syncButtons();
        }

        syncButtons() {
            this.metricButtons.forEach((button) => {
                button.classList.toggle("is-active", button.dataset.chartMetric === this.metric);
                button.setAttribute("aria-pressed", button.dataset.chartMetric === this.metric ? "true" : "false");
            });
            this.typeButtons.forEach((button) => {
                button.classList.toggle("is-active", button.dataset.chartType === this.type);
                button.setAttribute("aria-pressed", button.dataset.chartType === this.type ? "true" : "false");
            });
        }

        render() {
            this.type = this.normalizeType(this.type);
            const labels = Array.isArray(this.data.labels) ? this.data.labels : [];
            const series = this.data.series?.[this.metric];
            if (!labels.length || !series || !Array.isArray(series.values) || !series.values.length) {
                this.renderEmpty();
                return;
            }

            const values = series.values.map((value) => Number(value || 0));
            const maxValue = Math.max(...values, 1);
            const width = Math.max(this.stage.clientWidth || 0, 320);
            const height = 300;
            const padding = { top: 18, right: 18, bottom: 42, left: 18 };
            const innerHeight = height - padding.top - padding.bottom;
            const baseline = padding.top + innerHeight;
            const points = values.map((value, index) =>
                pointFor(index, value, values.length, width, height, padding, maxValue)
            );

            const gridLines = Array.from({ length: 5 }, (_, index) => {
                const ratio = index / 4;
                const y = padding.top + innerHeight * ratio;
                const gridValue = maxValue - maxValue * ratio;
                return [
                    svgEl("line", {
                        x1: padding.left,
                        y1: y,
                        x2: width - padding.right,
                        y2: y,
                        stroke: "rgba(20,33,42,0.10)",
                        "stroke-width": 1,
                    }),
                    svgEl(
                        "text",
                        {
                            x: width - padding.right,
                            y: y - 6,
                            "text-anchor": "end",
                            fill: "rgba(97,112,122,0.85)",
                            "font-size": 11,
                            "font-family": "Aptos, Bahnschrift, Segoe UI, sans-serif",
                        },
                        formatValue(gridValue, series.format)
                    ),
                ].join("");
            }).join("");

            const tickStep = Math.max(1, Math.ceil(labels.length / 7));
            const xLabels = labels
                .map((label, index) => {
                    if (index % tickStep !== 0 && index !== labels.length - 1) {
                        return "";
                    }
                    const point = points[index];
                    return svgEl(
                        "text",
                        {
                            x: point.x,
                            y: height - 14,
                            "text-anchor": "middle",
                            fill: "rgba(97,112,122,0.82)",
                            "font-size": 11,
                            "font-family": "Aptos, Bahnschrift, Segoe UI, sans-serif",
                        },
                        label
                    );
                })
                .join("");

            let seriesMarkup = "";
            if (this.type === "bar") {
                const innerWidth = width - padding.left - padding.right;
                const slotWidth = innerWidth / Math.max(values.length, 1);
                const barWidth = Math.max(Math.min(slotWidth * 0.58, 48), 12);
                seriesMarkup = points
                    .map((point, index) => {
                        const barHeight = baseline - point.y;
                        const x = point.x - barWidth / 2;
                        return svgEl(
                            "g",
                            {},
                            [
                                svgEl("title", {}, `${labels[index]}: ${formatValue(values[index], series.format)}`),
                                svgEl("rect", {
                                    x,
                                    y: point.y,
                                    width: barWidth,
                                    height: barHeight,
                                    rx: 9,
                                    fill: "rgba(15,106,92,0.88)",
                                }),
                            ].join("")
                        );
                    })
                    .join("");
            } else {
                const baseLinePath = linePath(points);
                const fillPath = areaPath(points, baseline);
                seriesMarkup = [
                    svgEl("path", {
                        d: fillPath,
                        fill: "rgba(15,106,92,0.16)",
                        stroke: "none",
                    }),
                    svgEl("path", {
                        d: baseLinePath,
                        fill: "none",
                        stroke: "#0f6a5c",
                        "stroke-width": 3,
                        "stroke-linecap": "round",
                        "stroke-linejoin": "round",
                    }),
                    points
                        .map((point, index) =>
                            svgEl(
                                "g",
                                {},
                                [
                                    svgEl("title", {}, `${labels[index]}: ${formatValue(values[index], series.format)}`),
                                    svgEl("circle", {
                                        cx: point.x,
                                        cy: point.y,
                                        r: 4.5,
                                        fill: "#ffffff",
                                        stroke: "#0f6a5c",
                                        "stroke-width": 2,
                                    }),
                                ].join("")
                            )
                        )
                        .join(""),
                ].join("");
            }

            this.stage.innerHTML = svgEl(
                "svg",
                {
                    class: "chart-svg",
                    viewBox: `0 0 ${width} ${height}`,
                    role: "img",
                    "aria-label": `${series.label}. ${labels[0]} — ${labels[labels.length - 1]}.`,
                },
                [gridLines, xLabels, seriesMarkup].join("")
            );

            const latest = values[values.length - 1] || 0;
            const peak = Math.max(...values, 0);
            const avg = average(values);
            this.caption.textContent = `${series.label} · ${labels[0]} — ${labels[labels.length - 1]}`;
            this.legend.innerHTML = [
                { label: "Последнее значение", value: latest },
                { label: "Среднее", value: avg },
                { label: "Пик", value: peak },
            ]
                .map(
                    (item) => `
                        <div class="legend-chip">
                            <span>${item.label}</span>
                            <strong>${formatValue(item.value, series.format)}</strong>
                        </div>
                    `
                )
                .join("");

            this.syncButtons();
        }
    }

    function resolveSafeReportsTab(root, requestedTab) {
        const availableTabs = Array.from(root.querySelectorAll("[data-reports-tab-trigger]")).map(
            (node) => node.dataset.reportsTabTrigger
        );
        const normalized = String(requestedTab || "").trim();
        if (normalized && availableTabs.includes(normalized)) {
            return normalized;
        }
        return availableTabs[0] || "overview";
    }

    function syncReportsTab(root, nextTab) {
        const activeTab = resolveSafeReportsTab(root, nextTab);
        root.dataset.activeTab = activeTab;

        root.querySelectorAll("[data-reports-tab-trigger]").forEach((trigger) => {
            const isActive = trigger.dataset.reportsTabTrigger === activeTab;
            trigger.classList.toggle("is-active", isActive);
            trigger.setAttribute("aria-selected", isActive ? "true" : "false");
            trigger.setAttribute("tabindex", isActive ? "0" : "-1");
        });

        root.querySelectorAll("[data-reports-panel]").forEach((panel) => {
            panel.hidden = panel.dataset.reportsPanel !== activeTab;
        });

        try {
            sessionStorage.setItem(REPORTS_TAB_STORAGE_KEY, activeTab);
        } catch (_error) {
            // Session storage can be unavailable.
        }
    }

    function initReportsTabs() {
        document.querySelectorAll("[data-reports-tabs-shell]").forEach((root) => {
            if (root.dataset.reportsTabsBound === "1") {
                return;
            }
            root.dataset.reportsTabsBound = "1";

            const triggers = Array.from(root.querySelectorAll("[data-reports-tab-trigger]"));
            triggers.forEach((trigger, index) => {
                trigger.addEventListener("click", () => {
                    syncReportsTab(root, trigger.dataset.reportsTabTrigger);
                    trigger.focus();
                });
                trigger.addEventListener("keydown", (event) => {
                    if (event.key !== "ArrowRight" && event.key !== "ArrowLeft") {
                        return;
                    }
                    event.preventDefault();
                    const direction = event.key === "ArrowRight" ? 1 : -1;
                    const nextIndex = (index + direction + triggers.length) % triggers.length;
                    triggers[nextIndex].click();
                });
            });

            let preferredTab = root.dataset.activeTab || "overview";
            try {
                preferredTab = sessionStorage.getItem(REPORTS_TAB_STORAGE_KEY) || preferredTab;
            } catch (_error) {
                // Session storage can be unavailable.
            }
            syncReportsTab(root, preferredTab);
        });
    }

    function initCharts() {
        document.querySelectorAll(".chart-widget").forEach((node) => {
            if (!node.dataset.chartBound) {
                node.dataset.chartBound = "1";
                new ChartWidget(node);
            }
        });
    }

    function bootstrap() {
        initReportsTabs();
        initCharts();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bootstrap);
    } else {
        bootstrap();
    }
})();
