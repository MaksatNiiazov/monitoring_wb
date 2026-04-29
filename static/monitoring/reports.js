(function () {
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
    const SERIES_PALETTE = [
        "#0f6a5c",
        "#22724d",
        "#986322",
        "#9d483d",
        "#345c8a",
        "#7a5af8",
        "#00838f",
        "#5f6b7a",
        "#ff7a59",
        "#d97706",
        "#2f855a",
        "#c026d3",
        "#2563eb",
        "#0ea5e9",
        "#9333ea",
        "#dc2626",
        "#475569",
        "#6b7280",
    ];
    const CAMPAIGN_SERIES_PALETTE = [
        "#0f766e",
        "#dc2626",
        "#2563eb",
        "#d97706",
        "#7c3aed",
        "#0891b2",
        "#65a30d",
        "#db2777",
        "#ea580c",
        "#0284c7",
        "#9333ea",
        "#16a34a",
    ];
    const DEFAULT_MULTI_SERIES = ["stock", "orders", "spend", "profit"];
    const DEFAULT_CAMPAIGN_METRICS = ["spend", "orders", "carts"];

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

    function formatInlineValue(value, format) {
        const number = Number(value || 0);
        if (format === "percent") {
            return `${PERCENT_FORMATTER.format(number)}%`;
        }
        if (format === "money" || format === "decimal") {
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

    function pointForRange(index, value, valuesCount, width, height, padding, minValue, maxValue) {
        const innerWidth = width - padding.left - padding.right;
        const innerHeight = height - padding.top - padding.bottom;
        const step = valuesCount > 1 ? innerWidth / (valuesCount - 1) : 0;
        const x = valuesCount > 1 ? padding.left + step * index : padding.left + innerWidth / 2;
        const span = maxValue - minValue;
        const normalized = span > 0 ? (value - minValue) / span : 0.5;
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
            this.seriesToggles = root.querySelector("[data-chart-series-toggles]");
            this.dynamicMetricGroup = root.querySelector("[data-chart-dynamic-metrics]");
            this.viewButtons = Array.from(root.querySelectorAll("[data-chart-view]"));
            this.hoverSeriesKey = null;

            const scriptId = root.dataset.chartScript;
            const scriptNode = document.getElementById(scriptId);
            this.rawData = scriptNode ? JSON.parse(scriptNode.textContent || "{}") : {};
            this.viewStates = {};
            this.activeViewKey = this.resolveInitialViewKey();

            this.viewButtons.forEach((button) => {
                button.addEventListener("click", () => this.setActiveView(button.dataset.chartView));
            });

            this.metricButtons.forEach((button) => {
                button.addEventListener("click", () => {
                    const state = this.ensureViewState(this.activeViewKey);
                    state.metric = button.dataset.chartMetric;
                    this.render();
                });
            });

            this.typeButtons.forEach((button) => {
                button.addEventListener("click", () => {
                    const state = this.ensureViewState(this.activeViewKey);
                    state.type = this.normalizeType(button.dataset.chartType);
                    this.render();
                });
            });

            if ("ResizeObserver" in window) {
                this.resizeObserver = new ResizeObserver(() => this.render());
                this.resizeObserver.observe(this.stage);
            } else {
                window.addEventListener("resize", () => this.render());
            }

            this.setActiveView(this.activeViewKey);
        }

        bindSeriesHover(elements, keyResolver) {
            elements.forEach((element) => {
                element.addEventListener("mouseenter", () => {
                    this.setHoveredSeries(keyResolver(element));
                });
                element.addEventListener("mouseleave", () => {
                    this.setHoveredSeries(null);
                });
                element.addEventListener("focus", () => {
                    this.setHoveredSeries(keyResolver(element));
                });
                element.addEventListener("blur", () => {
                    this.setHoveredSeries(null);
                });
            });
        }

        setHoveredSeries(nextKey) {
            const normalizedKey = nextKey || null;
            if (this.hoverSeriesKey === normalizedKey) {
                return;
            }
            this.hoverSeriesKey = normalizedKey;
            this.syncSeriesToggles();
            this.syncLegendHover();
            this.applySeriesHoverState();
        }

        matchesHoverTarget(target) {
            if (!this.hoverSeriesKey || !target) {
                return false;
            }
            const hoveredKey = String(this.hoverSeriesKey);
            if (hoveredKey.startsWith("metric:")) {
                return target.dataset.chartMetricKey === hoveredKey.slice("metric:".length);
            }
            if (hoveredKey.startsWith("campaign:")) {
                return target.dataset.chartCampaignKey === hoveredKey.slice("campaign:".length);
            }
            if (hoveredKey.startsWith("series:")) {
                return target.dataset.chartSeriesKey === hoveredKey.slice("series:".length);
            }
            return target.dataset.chartSeriesKey === hoveredKey;
        }

        syncLegendHover() {
            if (!this.legend) {
                return;
            }
            const hasHover = Boolean(this.hoverSeriesKey);
            this.legend.querySelectorAll("[data-chart-legend-series]").forEach((chip) => {
                const isHovered = hasHover && this.matchesHoverTarget(chip);
                const isDimmed = hasHover && !isHovered;
                chip.classList.toggle("is-hovered", isHovered);
                chip.classList.toggle("is-dimmed", isDimmed);
            });
        }

        applySeriesHoverState() {
            if (!this.stage) {
                return;
            }
            const hasHover = Boolean(this.hoverSeriesKey);
            this.stage.querySelectorAll("[data-chart-series-key]").forEach((group) => {
                const isHovered = hasHover && this.matchesHoverTarget(group);
                const isDimmed = hasHover && !isHovered;

                group.classList.toggle("is-hovered", isHovered);
                group.classList.toggle("is-dimmed", isDimmed);

                group.querySelectorAll("[data-chart-series-path]").forEach((node) => {
                    node.setAttribute("stroke-width", isHovered ? "4" : "2.6");
                    node.setAttribute("stroke-opacity", isDimmed ? "0.2" : (isHovered ? "1" : "0.96"));
                });
                group.querySelectorAll("[data-chart-series-point]").forEach((node) => {
                    node.setAttribute("opacity", isDimmed ? "0.3" : "1");
                });
                group.querySelectorAll("[data-chart-series-label]").forEach((node) => {
                    const labelOpacity = hasHover ? (isHovered ? "1" : "0.45") : "0.88";
                    node.setAttribute("opacity", labelOpacity);
                    node.setAttribute("font-size", isHovered ? "14" : "12");
                });
            });
        }

        resolveInitialViewKey() {
            const viewKeys = this.getViewKeys();
            return this.rawData.defaultView || viewKeys[0] || "default";
        }

        getViewKeys() {
            return this.rawData.views ? Object.keys(this.rawData.views) : [];
        }

        getViewData(viewKey = this.activeViewKey) {
            if (this.rawData.views) {
                return this.rawData.views[viewKey] || this.rawData.views[this.getViewKeys()[0]] || {};
            }
            return this.rawData;
        }

        normalizeType(nextType) {
            const normalized = String(nextType || "").toLowerCase();
            if (normalized === "bar" || normalized === "line") {
                return normalized;
            }
            return "line";
        }

        resolveMode(viewData) {
            return String(this.root.dataset.chartMode || viewData.mode || "single").toLowerCase();
        }

        resolveSeriesColor(series, index) {
            if (series?.color) {
                return series.color;
            }
            return SERIES_PALETTE[index % SERIES_PALETTE.length];
        }

        resolveSeriesFormat(series, metricKey = this.ensureViewState(this.activeViewKey).metric) {
            return series?.format || this.data.metrics?.[metricKey]?.format || "int";
        }

        getWindowLabel(labels) {
            if (this.data.windowLabel) {
                return this.data.windowLabel;
            }
            if (!labels.length) {
                return "";
            }
            return `${labels[0]} — ${labels[labels.length - 1]}`;
        }

        getAvailableMetricMap(viewData) {
            if (this.resolveMode(viewData) === "campaigns") {
                return viewData.metrics || {};
            }
            return null;
        }

        getMetricOrder(viewData) {
            if (this.resolveMode(viewData) === "campaigns") {
                const metricMap = this.getAvailableMetricMap(viewData);
                return viewData.metricOrder || Object.keys(metricMap || {});
            }
            return viewData.seriesOrder || Object.keys(viewData.series || {});
        }

        getAvailableSeries(viewData, metric) {
            if (this.resolveMode(viewData) === "campaigns") {
                return viewData.metrics?.[metric]?.series || {};
            }
            return viewData.series || {};
        }

        getSeriesOrder(viewData, metric) {
            if (this.resolveMode(viewData) === "campaigns") {
                return viewData.metrics?.[metric]?.seriesOrder || Object.keys(this.getAvailableSeries(viewData, metric));
            }
            return viewData.seriesOrder || Object.keys(this.getAvailableSeries(viewData, metric));
        }

        getCampaignOrder(viewData) {
            if (this.resolveMode(viewData) !== "campaigns") {
                return [];
            }
            const metricOrder = this.getMetricOrder(viewData);
            for (const metricKey of metricOrder) {
                const order = this.getSeriesOrder(viewData, metricKey);
                if (order.length) {
                    return order;
                }
            }
            return [];
        }

        resolveDefaultSeries(viewData, seriesOrder, initialMetric) {
            const mode = this.resolveMode(viewData);
            const providedDefaults = Array.isArray(viewData.defaultSeries)
                ? viewData.defaultSeries
                : [];
            const preferredDefaults = mode === "campaigns" ? DEFAULT_CAMPAIGN_METRICS : DEFAULT_MULTI_SERIES;
            const filterKeys = (keys) => {
                const seenKeys = new Set();
                return keys.filter((key) => {
                    if (!key || seenKeys.has(key)) {
                        return false;
                    }
                    seenKeys.add(key);
                    if (mode === "campaigns") {
                        return Boolean(this.getAvailableMetricMap(viewData)?.[key]);
                    }
                    return Boolean(this.getAvailableSeries(viewData, initialMetric)?.[key]);
                });
            };
            const defaultKeys = filterKeys(providedDefaults.length ? providedDefaults : preferredDefaults);
            if (defaultKeys.length) {
                return defaultKeys;
            }
            return filterKeys(seriesOrder).slice(0, 1);
        }

        ensureViewState(viewKey) {
            if (this.viewStates[viewKey]) {
                return this.viewStates[viewKey];
            }
            const viewData = this.getViewData(viewKey);
            const metricOrder = this.getMetricOrder(viewData);
            const initialMetric = viewData.defaultMetric || metricOrder[0] || this.metricButtons[0]?.dataset.chartMetric || "orders";
            const seriesOrder = this.resolveMode(viewData) === "campaigns"
                ? metricOrder
                : this.getSeriesOrder(viewData, initialMetric);
            const defaultSeries = this.resolveDefaultSeries(viewData, seriesOrder, initialMetric);
            this.viewStates[viewKey] = {
                metric: initialMetric,
                type: this.resolveMode(viewData) === "multi" || this.resolveMode(viewData) === "campaigns"
                    ? "line"
                    : this.normalizeType(viewData.defaultType || "line"),
                activeSeries: new Set(defaultSeries.filter((key) =>
                    this.resolveMode(viewData) === "campaigns"
                        ? Boolean(this.getAvailableMetricMap(viewData)?.[key])
                        : Boolean(this.getAvailableSeries(viewData, initialMetric)?.[key])
                )),
                activeCampaigns: new Set(
                    this.resolveMode(viewData) === "campaigns"
                        ? this.getCampaignOrder(viewData)
                        : []
                ),
            };
            return this.viewStates[viewKey];
        }

        setActiveView(viewKey) {
            this.hoverSeriesKey = null;
            this.activeViewKey = viewKey;
            this.data = this.getViewData(viewKey);
            this.mode = this.resolveMode(this.data);
            this.root.dataset.chartActiveMode = this.mode;
            const state = this.ensureViewState(viewKey);
            this.metric = state.metric;
            this.type = state.type;
            this.syncViewButtons();
            this.syncDynamicMetricButtons();
            this.buildSeriesToggles();
            this.render();
        }

        syncViewButtons() {
            this.viewButtons.forEach((button) => {
                const isActive = button.dataset.chartView === this.activeViewKey;
                button.classList.toggle("is-active", isActive);
                button.setAttribute("aria-pressed", isActive ? "true" : "false");
            });
        }

        syncDynamicMetricButtons() {
            if (!this.dynamicMetricGroup) {
                return;
            }
            if (this.mode !== "campaigns") {
                this.dynamicMetricGroup.hidden = true;
                this.dynamicMetricGroup.innerHTML = "";
                return;
            }
            const state = this.ensureViewState(this.activeViewKey);
            const metricMap = this.getAvailableMetricMap(this.data);
            const metricOrder = this.getMetricOrder(this.data);
            this.dynamicMetricGroup.hidden = false;
            this.dynamicMetricGroup.innerHTML = metricOrder
                .filter((key) => metricMap[key])
                .map((key, index) => {
                    const metric = metricMap[key];
                    const isActive = state.activeSeries.has(key);
                    const campaignOrder = metric.seriesOrder || Object.keys(metric.series || {});
                    const values = campaignOrder.flatMap((campaignKey) =>
                        (metric.series?.[campaignKey]?.values || []).map((value) => Number(value || 0))
                    );
                    const avgValue = average(values);
                    const color = metric.color || this.resolveSeriesColor(metric, index);
                    return `
                        <button
                            type="button"
                            class="chart-series-toggle${isActive ? " is-active" : ""}"
                            data-chart-dynamic-metric="${key}"
                            aria-pressed="${isActive ? "true" : "false"}"
                        >
                            <span class="chart-series-toggle-head">
                                <span class="chart-series-swatch" style="--series-color:${color}"></span>
                                <span class="chart-series-toggle-label">${metric.label || key}</span>
                            </span>
                            <strong class="chart-series-toggle-value">${formatValue(avgValue, metric.format || "int")}</strong>
                        </button>
                    `;
                })
                .join("");
            this.bindSeriesHover(
                Array.from(this.dynamicMetricGroup.querySelectorAll("[data-chart-dynamic-metric]")),
                (button) => `metric:${button.dataset.chartDynamicMetric}`
            );
            this.dynamicMetricGroup.querySelectorAll("[data-chart-dynamic-metric]").forEach((button) => {
                button.addEventListener("click", () => {
                    const key = button.dataset.chartDynamicMetric;
                    const nextState = this.ensureViewState(this.activeViewKey);
                    if (nextState.activeSeries.has(key) && nextState.activeSeries.size === 1) {
                        return;
                    }
                    if (nextState.activeSeries.has(key)) {
                        nextState.activeSeries.delete(key);
                    } else {
                        nextState.activeSeries.add(key);
                    }
                    this.syncDynamicMetricButtons();
                    this.render();
                });
            });
        }

        buildSeriesToggles() {
            if (!this.seriesToggles) {
                return;
            }
            const state = this.ensureViewState(this.activeViewKey);
            if (this.mode === "campaigns") {
                const metricMap = this.getAvailableMetricMap(this.data);
                const campaignOrder = this.getCampaignOrder(this.data);
                const activeMetricKeys = this.getMetricOrder(this.data).filter((key) => state.activeSeries.has(key) && metricMap[key]);

                if (!campaignOrder.length) {
                    this.seriesToggles.innerHTML = "";
                    return;
                }

                this.seriesToggles.innerHTML = campaignOrder
                    .map((campaignKey, index) => {
                        const sampleMetric = activeMetricKeys[0] ? metricMap[activeMetricKeys[0]] : metricMap[this.getMetricOrder(this.data)[0]];
                        const sampleSeries = sampleMetric?.series?.[campaignKey];
                        const isActive = state.activeCampaigns.has(campaignKey);
                        const sampleColor = this.resolveCampaignEntryColor(sampleMetric, sampleSeries, 0, index, 1);
                        const totals = Array.from({ length: (this.data.labels || []).length }, (_value, labelIndex) =>
                            activeMetricKeys.reduce((sum, metricKey) => {
                                const metric = metricMap[metricKey];
                                const campaignSeries = metric?.series?.[campaignKey];
                                return sum + Number(campaignSeries?.values?.[labelIndex] || 0);
                            }, 0)
                        );
                        return `
                            <button
                                type="button"
                                class="chart-series-toggle${isActive ? " is-active" : ""}"
                                data-chart-series-toggle="${campaignKey}"
                                data-chart-series-key="${campaignKey}"
                                aria-pressed="${isActive ? "true" : "false"}"
                            >
                                <span class="chart-series-toggle-head">
                                    <span class="chart-series-swatch" style="--series-color:${sampleColor}"></span>
                                    <span class="chart-series-toggle-label">${sampleSeries?.label || campaignKey}</span>
                                </span>
                                <strong class="chart-series-toggle-value">${activeMetricKeys.length ? activeMetricKeys.length : 0}</strong>
                            </button>
                        `;
                    })
                    .join("");

                this.seriesToggles.querySelectorAll("[data-chart-series-toggle]").forEach((button) => {
                    button.addEventListener("click", () => {
                        const key = button.dataset.chartSeriesToggle;
                        const nextState = this.ensureViewState(this.activeViewKey);
                        if (nextState.activeCampaigns.has(key) && nextState.activeCampaigns.size === 1) {
                            return;
                        }
                        if (nextState.activeCampaigns.has(key)) {
                            nextState.activeCampaigns.delete(key);
                        } else {
                            nextState.activeCampaigns.add(key);
                        }
                        this.syncSeriesToggles();
                        this.render();
                    });
                });

                this.bindSeriesHover(
                    Array.from(this.seriesToggles.querySelectorAll("[data-chart-series-toggle]")),
                    (button) => `campaign:${button.dataset.chartSeriesKey}`
                );

                this.syncSeriesToggles();
                return;
            }
            const seriesMap = this.getAvailableSeries(this.data, state.metric);
            const seriesOrder = this.getSeriesOrder(this.data, state.metric);

            if (!seriesOrder.length) {
                this.seriesToggles.innerHTML = "";
                return;
            }

            this.seriesToggles.innerHTML = seriesOrder
                .filter((key) => seriesMap[key])
                .map((key, index) => {
                    const series = seriesMap[key];
                    const color = this.resolveSeriesColor(series, index);
                    const isActive = state.activeSeries.has(key);
                    const values = (series.values || []).map((value) => Number(value || 0));
                    const avgValue = average(values);
                    const formatName = this.resolveSeriesFormat(series, state.metric);
                    return `
                        <button
                            type="button"
                            class="chart-series-toggle${isActive ? " is-active" : ""}"
                            data-chart-series-toggle="${key}"
                            data-chart-series-key="${key}"
                            aria-pressed="${isActive ? "true" : "false"}"
                        >
                            <span class="chart-series-toggle-head">
                                <span class="chart-series-swatch" style="--series-color:${color}"></span>
                                <span class="chart-series-toggle-label">${series.label || key}</span>
                            </span>
                            <strong class="chart-series-toggle-value">${formatValue(avgValue, formatName)}</strong>
                        </button>
                    `;
                })
                .join("");

            this.seriesToggles.querySelectorAll("[data-chart-series-toggle]").forEach((button) => {
                button.addEventListener("click", () => {
                    const key = button.dataset.chartSeriesToggle;
                    const nextState = this.ensureViewState(this.activeViewKey);
                    if (nextState.activeSeries.has(key) && nextState.activeSeries.size === 1) {
                        return;
                    }
                    if (nextState.activeSeries.has(key)) {
                        nextState.activeSeries.delete(key);
                    } else {
                        nextState.activeSeries.add(key);
                    }
                    this.syncSeriesToggles();
                    this.render();
                });
            });

            this.bindSeriesHover(
                Array.from(this.seriesToggles.querySelectorAll("[data-chart-series-toggle]")),
                (button) => `series:${button.dataset.chartSeriesKey}`
            );

            this.syncSeriesToggles();
        }

        syncSeriesToggles() {
            if (!this.seriesToggles) {
                return;
            }
            const state = this.ensureViewState(this.activeViewKey);
            this.seriesToggles.querySelectorAll("[data-chart-series-toggle]").forEach((button) => {
                const key = button.dataset.chartSeriesToggle;
                const isActive = this.mode === "campaigns"
                    ? state.activeCampaigns.has(key)
                    : state.activeSeries.has(key);
                const hoverKey = this.mode === "campaigns" ? `campaign:${key}` : `series:${key}`;
                const isHovered = Boolean(this.hoverSeriesKey) && this.hoverSeriesKey === hoverKey;
                const isDimmed = Boolean(this.hoverSeriesKey) && !isHovered;
                button.classList.toggle("is-active", isActive);
                button.classList.toggle("is-hovered", isHovered);
                button.classList.toggle("is-dimmed", isDimmed);
                button.setAttribute("aria-pressed", isActive ? "true" : "false");
            });
        }

        renderEmpty(options = {}) {
            const originalEmptyText = this.data.emptyText;
            const originalEmptyCaption = this.data.emptyCaption;
            if (Object.prototype.hasOwnProperty.call(options, "text")) {
                this.data.emptyText = options.text;
            }
            if (Object.prototype.hasOwnProperty.call(options, "caption")) {
                this.data.emptyCaption = options.caption;
            }
            this.stage.innerHTML = `<div class="chart-empty">${this.data.emptyText || "Данных пока недостаточно для построения графика."}</div>`;
            this.caption.textContent = this.data.emptyCaption || "Сначала выполните синхронизацию и сформируйте срез за выбранный период.";
            this.legend.innerHTML = "";
            this.syncButtons();
            this.data.emptyText = originalEmptyText;
            this.data.emptyCaption = originalEmptyCaption;
        }

        renderSeriesSelectionEmpty() {
            this.renderEmpty({
                text: "Выберите показатели, чтобы построить график.",
                caption: "",
            });
        }

        syncButtons() {
            this.syncViewButtons();
            this.syncDynamicMetricButtons();
            if (this.mode === "multi" || this.mode === "campaigns") {
                this.syncSeriesToggles();
                return;
            }
            const state = this.ensureViewState(this.activeViewKey);
            this.metricButtons.forEach((button) => {
                const isActive = button.dataset.chartMetric === state.metric;
                button.classList.toggle("is-active", isActive);
                button.setAttribute("aria-pressed", isActive ? "true" : "false");
            });
            this.typeButtons.forEach((button) => {
                const isActive = button.dataset.chartType === state.type;
                button.classList.toggle("is-active", isActive);
                button.setAttribute("aria-pressed", isActive ? "true" : "false");
            });
        }

        renderSingle() {
            const state = this.ensureViewState(this.activeViewKey);
            this.type = this.normalizeType(state.type);
            const labels = Array.isArray(this.data.labels) ? this.data.labels : [];
            const series = this.data.series?.[state.metric];
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

        renderMultiSeries(labels, seriesEntries, captionText) {
            if (!labels.length || !seriesEntries.length) {
                this.renderEmpty();
                return;
            }

            const width = Math.max(this.stage.clientWidth || 0, 860);
            const height = Math.max(460, 250 + seriesEntries.length * 12);
            const padding = { top: 26, right: 20, bottom: 54, left: 20 };
            const innerHeight = height - padding.top - padding.bottom;
            const baseline = padding.top + innerHeight;
            const tickStep = Math.max(1, Math.ceil(labels.length / 10));

            const gridLines = Array.from({ length: 5 }, (_, index) => {
                const ratio = index / 4;
                const y = padding.top + innerHeight * ratio;
                return svgEl("line", {
                    x1: padding.left,
                    y1: y,
                    x2: width - padding.right,
                    y2: y,
                    stroke: "rgba(20,33,42,0.10)",
                    "stroke-width": 1,
                });
            }).join("");

            const xLabels = labels
                .map((label, index) => {
                    if (index % tickStep !== 0 && index !== labels.length - 1) {
                        return "";
                    }
                    const point = pointForRange(index, 0, labels.length, width, height, padding, 0, 1);
                    return svgEl(
                        "text",
                        {
                            x: point.x,
                            y: height - 16,
                            "text-anchor": "middle",
                            fill: "rgba(97,112,122,0.82)",
                            "font-size": 11,
                            "font-family": "Aptos, Bahnschrift, Segoe UI, sans-serif",
                        },
                        label
                    );
                })
                .join("");

            const seriesMarkup = seriesEntries
                .map((series, seriesIndex) => {
                    const points = series.values.map((value, valueIndex) =>
                        pointForRange(
                            valueIndex,
                            value,
                            series.values.length,
                            width,
                            height,
                            padding,
                            series.minValue,
                            series.maxValue
                        )
                    );
                    const labelsMarkup = points
                        .map((point, index) => {
                            const lift = 14 + (seriesIndex % 4) * 12;
                            const textY = Math.max(padding.top + 12, point.y - lift);
                            return svgEl(
                                "g",
                                {
                                    "data-chart-series-point": "",
                                },
                                [
                                    svgEl("circle", {
                                        cx: point.x,
                                        cy: point.y,
                                        r: 3.7,
                                        fill: "#ffffff",
                                        stroke: series.color,
                                        "stroke-width": 2,
                                    }),
                                    svgEl(
                                        "text",
                                        {
                                            x: point.x,
                                            y: textY,
                                            "text-anchor": "middle",
                                            fill: series.color,
                                            "font-size": 12,
                                            "font-weight": 800,
                                            "font-family": "Aptos, Bahnschrift, Segoe UI, sans-serif",
                                            stroke: "rgba(255,255,255,0.96)",
                                            "stroke-width": 4,
                                            "paint-order": "stroke",
                                            opacity: 0.88,
                                            "data-chart-series-label": "",
                                        },
                                        formatInlineValue(series.values[index], series.format)
                                    ),
                                ].join("")
                            );
                        })
                        .join("");

                    return svgEl(
                        "g",
                        {
                            "data-chart-series-key": series.key,
                            "data-chart-metric-key": series.metricKey || "",
                            "data-chart-campaign-key": series.campaignKey || "",
                        },
                        [
                            svgEl("path", {
                                d: linePath(points),
                                fill: "none",
                                stroke: series.color,
                                "stroke-width": 2.6,
                                "stroke-linecap": "round",
                                "stroke-linejoin": "round",
                                "stroke-opacity": 0.96,
                                "data-chart-series-path": "",
                            }),
                            labelsMarkup,
                        ].join("")
                    );
                })
                .join("");

            this.stage.innerHTML = svgEl(
                "svg",
                {
                    class: "chart-svg",
                    viewBox: `0 0 ${width} ${height}`,
                    role: "img",
                    "aria-label": captionText,
                },
                [
                    gridLines,
                    svgEl("line", {
                        x1: padding.left,
                        y1: baseline,
                        x2: width - padding.right,
                        y2: baseline,
                        stroke: "rgba(20,33,42,0.14)",
                        "stroke-width": 1,
                    }),
                    xLabels,
                    seriesMarkup,
                ].join("")
            );
            this.applySeriesHoverState();
        }

        resolveCampaignEntryColor(metric, campaignSeries, metricIndex, campaignIndex, selectedMetricCount) {
            if (selectedMetricCount <= 1) {
                return CAMPAIGN_SERIES_PALETTE[campaignIndex % CAMPAIGN_SERIES_PALETTE.length];
            }
            return CAMPAIGN_SERIES_PALETTE[(metricIndex * 5 + campaignIndex) % CAMPAIGN_SERIES_PALETTE.length];
        }

        buildSeriesEntriesFromCurrentView(formatOverride = null) {
            const state = this.ensureViewState(this.activeViewKey);
            if (this.mode === "campaigns") {
                const metricMap = this.getAvailableMetricMap(this.data);
                const metricOrder = this.getMetricOrder(this.data);
                const activeMetricKeys = metricOrder.filter((key) => state.activeSeries.has(key) && metricMap[key]);
                const selectedMetricCount = activeMetricKeys.length;
                return activeMetricKeys.flatMap((metricKey, metricIndex) => {
                    const metric = metricMap[metricKey];
                    const campaignOrder = metric.seriesOrder || Object.keys(metric.series || {});
                    return campaignOrder
                        .filter((campaignKey) => state.activeCampaigns.has(campaignKey) && metric.series?.[campaignKey])
                        .map((campaignKey, campaignIndex) => {
                            const campaignSeries = metric.series[campaignKey];
                            const values = (campaignSeries.values || []).map((value) => Number(value || 0));
                            return {
                                key: `${metricKey}::${campaignKey}`,
                                metricKey,
                                campaignKey,
                                label: selectedMetricCount > 1
                                    ? `${metric.label || metricKey} · ${campaignSeries.label || campaignKey}`
                                    : (campaignSeries.label || campaignKey),
                                format: metric.format || formatOverride || "int",
                                values,
                                color: this.resolveCampaignEntryColor(metric, campaignSeries, metricIndex, campaignIndex, selectedMetricCount),
                                minValue: Math.min(...values),
                                maxValue: Math.max(...values),
                            };
                        });
                });
            }
            const seriesMap = this.getAvailableSeries(this.data, state.metric);
            const seriesOrder = this.getSeriesOrder(this.data, state.metric);
            return seriesOrder
                .filter((key) => state.activeSeries.has(key) && seriesMap[key])
                .map((key, index) => {
                    const series = seriesMap[key];
                    const values = (series.values || []).map((value) => Number(value || 0));
                    return {
                        key,
                        label: series.label || key,
                        format: formatOverride || this.resolveSeriesFormat(series, state.metric),
                        values,
                        color: this.resolveSeriesColor(series, index),
                        minValue: Math.min(...values),
                        maxValue: Math.max(...values),
                    };
                });
        }

        renderAverageLegend(seriesEntries) {
            this.legend.innerHTML = seriesEntries
                .map((series) => `
                    <div
                        class="legend-chip legend-chip-series legend-chip-average"
                        data-chart-legend-series="${series.key}"
                        data-chart-series-key="${series.key}"
                        data-chart-metric-key="${series.metricKey || ""}"
                        data-chart-campaign-key="${series.campaignKey || ""}"
                        tabindex="0"
                    >
                        <span class="legend-series-label">
                            <span class="chart-series-swatch" style="--series-color:${series.color}"></span>
                            <span>${series.label}</span>
                        </span>
                        <strong>${formatValue(average(series.values), series.format)}</strong>
                    </div>
                `)
                .join("");

            this.bindSeriesHover(
                Array.from(this.legend.querySelectorAll("[data-chart-legend-series]")),
                (chip) => `series:${chip.dataset.chartLegendSeries}`
            );
            this.syncLegendHover();
        }

        renderMulti() {
            const labels = Array.isArray(this.data.labels) ? this.data.labels : [];
            const seriesEntries = this.buildSeriesEntriesFromCurrentView();
            const windowLabel = this.getWindowLabel(labels);

            if (!labels.length) {
                this.renderEmpty();
                return;
            }
            if (!seriesEntries.length) {
                this.renderSeriesSelectionEmpty();
                return;
            }

            this.renderMultiSeries(
                labels,
                seriesEntries,
                `Графики по товару за период ${windowLabel}.`
            );

            if (!labels.length || !seriesEntries.length) {
                return;
            }

            this.caption.textContent = `Средние значения за период ${windowLabel}.`;
            this.renderAverageLegend(seriesEntries);

            this.syncButtons();
        }

        renderCampaigns() {
            const labels = Array.isArray(this.data.labels) ? this.data.labels : [];
            const state = this.ensureViewState(this.activeViewKey);
            const seriesEntries = this.buildSeriesEntriesFromCurrentView();
            const windowLabel = this.getWindowLabel(labels);
            const metricMap = this.getAvailableMetricMap(this.data);
            const selectedMetricLabels = this.getMetricOrder(this.data)
                .filter((key) => state.activeSeries.has(key) && metricMap[key])
                .map((key) => metricMap[key].label || key);
            const metricData = {
                label: selectedMetricLabels.length ? selectedMetricLabels.join(", ") : "метрики",
            };

            if (!labels.length) {
                this.renderEmpty();
                return;
            }
            if (!seriesEntries.length) {
                this.renderSeriesSelectionEmpty();
                return;
            }

            this.renderMultiSeries(
                labels,
                seriesEntries,
                `График рекламных кампаний за период ${windowLabel}.`
            );

            if (!labels.length || !seriesEntries.length) {
                return;
            }

            this.caption.textContent = `${metricData?.label || "Метрика"} по рекламным кампаниям за период ${windowLabel}. Ниже показаны средние значения по каждой РК.`;
            this.renderAverageLegend(seriesEntries);

            this.syncButtons();
        }

        render() {
            this.data = this.getViewData(this.activeViewKey);
            this.mode = this.resolveMode(this.data);
            this.root.dataset.chartActiveMode = this.mode;
            if (this.mode === "multi") {
                this.renderMulti();
                return;
            }
            if (this.mode === "campaigns") {
                this.renderCampaigns();
                return;
            }
            this.renderSingle();
        }
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
        initCharts();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bootstrap);
    } else {
        bootstrap();
    }
})();
