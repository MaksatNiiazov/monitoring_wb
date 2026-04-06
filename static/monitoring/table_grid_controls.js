(function () {
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

    async function saveCell(updateUrl, payload) {
        const response = await fetch(updateUrl, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Accept: "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRFToken": getCookie("csrftoken"),
            },
            body: JSON.stringify(payload),
            cache: "no-store",
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data.ok) {
            const detail = data && data.detail ? data.detail : `HTTP ${response.status}`;
            throw new Error(detail);
        }
        return data;
    }

    function parseNumericValue(rawValue) {
        if (rawValue === null || typeof rawValue === "undefined") {
            return null;
        }
        const text = String(rawValue).trim();
        if (!text || text === "-" || text === "—") {
            return null;
        }
        const cleaned = text
            .replace(/\u00a0/g, "")
            .replace(/\s/g, "")
            .replace(/руб\.?/gi, "")
            .replace(/₽/g, "")
            .replace(/%/g, "")
            .replace(/,/g, ".");
        const value = Number.parseFloat(cleaned);
        return Number.isFinite(value) ? value : null;
    }

    function formatDecimalValue(value) {
        if (!Number.isFinite(value)) {
            return "";
        }
        let text = value.toFixed(2);
        text = text.replace(/\.?0+$/, "");
        return text.replace(".", ",");
    }

    function formatPercentValue(value) {
        if (!Number.isFinite(value)) {
            return "";
        }
        if (value !== 0 && Math.abs(value) < 0.01) {
            return value > 0 ? "<0,01%" : ">-0,01%";
        }
        return `${formatDecimalValue(value)}%`;
    }

    function formatRatioCell(numerator, denominator, { percent = false, blankWhenZeroNumerator = false } = {}) {
        if (!Number.isFinite(denominator) || denominator === 0) {
            return "-";
        }
        if (!Number.isFinite(numerator)) {
            return "-";
        }
        if (blankWhenZeroNumerator && numerator === 0) {
            return "-";
        }
        const value = numerator / denominator;
        return percent ? formatPercentValue(value * 100) : formatDecimalValue(value);
    }

    function initLiveCalculations(tableWrap) {
        const table = tableWrap.querySelector("table");
        if (!table) {
            return;
        }

        const ROW = {
            SPEND: 5,
            IMPRESSIONS: 6,
            CTR: 7,
            CPM: 8,
            CPC: 9,
            CLICKS: 10,
            CARTS: 11,
            CONVERSION_CART: 12,
            ORDERS: 13,
            CONVERSION_ORDER: 14,
            ORDER_SUM: 15,
            BUYOUTS: 16,
            DRR_SALES: 20,
            PROFIT: 21,
            BUYOUT_PERCENT: 22,
            UNIT_COST: 23,
            LOGISTICS: 24,
            STOCK_TOTAL: 26,
            AVG_STOCK_DROP: 30,
            DAYS_UNTIL_ZERO: 31,
            SELLER_PRICE: 36,
        };
        const COL = {
            OVERALL: 4,
            INPUT_MAIN: 1,
            SELLER_PRICE: 5,
            STOCK: 4,
        };

        const getCell = (row, block, inBlockCol) =>
            table.querySelector(`td[data-row="${row}"][data-block="${block}"][data-in-block-col="${inBlockCol}"]`);

        const readNumber = (row, block, inBlockCol) => {
            const cell = getCell(row, block, inBlockCol);
            if (!cell) {
                return null;
            }
            const input = cell.querySelector("input");
            if (input) {
                return parseNumericValue(input.value);
            }
            const span = cell.querySelector("span");
            return parseNumericValue(span ? span.textContent : cell.textContent);
        };

        const setCellText = (row, block, inBlockCol, value) => {
            const cell = getCell(row, block, inBlockCol);
            if (!cell) {
                return;
            }
            const span = cell.querySelector("span");
            if (span) {
                span.textContent = value;
                return;
            }
            cell.textContent = value;
        };

        const resolveBlocksCount = () => {
            let max = -1;
            table.querySelectorAll("td[data-block]").forEach((cell) => {
                const raw = Number.parseInt(cell.dataset.block || "", 10);
                if (Number.isFinite(raw)) {
                    max = Math.max(max, raw);
                }
            });
            return max + 1;
        };

        const recalcBlock = (blockIndex) => {
            const impressions = readNumber(ROW.IMPRESSIONS, blockIndex, COL.OVERALL);
            const clicks = readNumber(ROW.CLICKS, blockIndex, COL.OVERALL);
            const carts = readNumber(ROW.CARTS, blockIndex, COL.OVERALL);
            const orders = readNumber(ROW.ORDERS, blockIndex, COL.OVERALL);
            const orderSum = readNumber(ROW.ORDER_SUM, blockIndex, COL.OVERALL);
            const spend = readNumber(ROW.SPEND, blockIndex, COL.OVERALL);

            setCellText(ROW.CTR, blockIndex, COL.OVERALL, "-");
            setCellText(ROW.CPM, blockIndex, COL.OVERALL, "-");

            setCellText(
                ROW.CPC,
                blockIndex,
                COL.OVERALL,
                formatRatioCell(spend, clicks),
            );

            const conversionCart = clicks ? (carts || 0) * 100 / clicks : null;
            setCellText(ROW.CONVERSION_CART, blockIndex, COL.OVERALL, Number.isFinite(conversionCart) ? formatPercentValue(conversionCart) : "-");

            const conversionOrder = carts ? (orders || 0) * 100 / carts : null;
            setCellText(ROW.CONVERSION_ORDER, blockIndex, COL.OVERALL, Number.isFinite(conversionOrder) ? formatPercentValue(conversionOrder) : "-");

            const buyoutPercent = readNumber(ROW.BUYOUT_PERCENT, blockIndex, COL.INPUT_MAIN) || 0;
            const buyoutFraction = Math.abs(buyoutPercent) > 1 ? buyoutPercent / 100 : buyoutPercent;
            const buyouts = orderSum && buyoutFraction ? orderSum * buyoutFraction : null;
            setCellText(ROW.BUYOUTS, blockIndex, COL.OVERALL, Number.isFinite(buyouts) ? formatDecimalValue(buyouts) : "-");

            const drrSalesCell = formatRatioCell(spend, buyouts, { percent: true, blankWhenZeroNumerator: true });
            setCellText(ROW.DRR_SALES, blockIndex, COL.OVERALL, drrSalesCell);

            const sellerPrice = readNumber(ROW.SELLER_PRICE, blockIndex, COL.SELLER_PRICE) || 0;
            const unitCost = readNumber(ROW.UNIT_COST, blockIndex, COL.INPUT_MAIN) || 0;
            const logistics = readNumber(ROW.LOGISTICS, blockIndex, COL.INPUT_MAIN) || 0;
            const totalOrders = orders || 0;

            let profit = null;
            if (sellerPrice && buyoutFraction > 0) {
                const drrRatioForProfit =
                    Number.isFinite(spend) && spend > 0 && Number.isFinite(buyouts) && buyouts > 0
                        ? spend / buyouts
                        : 0;
                const logisticsAdjustment = logistics / buyoutFraction - 50;
                const margin =
                    sellerPrice -
                    unitCost -
                    (sellerPrice * drrRatioForProfit) -
                    sellerPrice * 0.25 -
                    logisticsAdjustment;
                profit = margin * totalOrders * buyoutFraction;
            }
            setCellText(ROW.PROFIT, blockIndex, COL.INPUT_MAIN, Number.isFinite(profit) ? formatDecimalValue(profit) : "-");
        };

        const recalcAll = () => {
            const blocksCount = resolveBlocksCount();
            for (let blockIndex = 0; blockIndex < blocksCount; blockIndex += 1) {
                recalcBlock(blockIndex);
            }
        };

        tableWrap._recalculateBlock = recalcBlock;
        tableWrap._recalculateAllBlocks = recalcAll;

        recalcAll();
    }

    function setBoolPending(wrapper, pending) {
        wrapper.classList.toggle("is-pending", pending);
        wrapper.querySelectorAll(".grid-choice-btn").forEach((button) => {
            button.disabled = pending;
        });
    }

    function setBoolActive(wrapper, boolValue) {
        wrapper.querySelectorAll(".grid-choice-btn").forEach((button) => {
            const value = button.dataset.value === "true";
            button.classList.toggle("is-active", value === boolValue);
        });
    }

    function setSelectPending(select, pending) {
        select.classList.toggle("is-pending", pending);
        select.disabled = pending;
    }

    function setInputPending(input, pending) {
        input.classList.toggle("is-pending", pending);
        input.disabled = pending;
    }

    function setKeywordRowsPending(wrapper, pending) {
        wrapper.classList.toggle("is-pending", pending);
        wrapper.querySelectorAll(".grid-keyword-action").forEach((button) => {
            button.disabled = pending;
        });
    }

    function initInlineControls(tableWrap, updateUrl) {
        const statusNode = document.querySelector("[data-inline-status]");
        let statusTimer = 0;
        const showStatus = (message, tone) => {
            if (!statusNode) {
                return;
            }
            window.clearTimeout(statusTimer);
            statusNode.hidden = false;
            statusNode.dataset.tone = tone || "neutral";
            statusNode.textContent = message;
            if (tone !== "error") {
                statusTimer = window.setTimeout(() => {
                    statusNode.hidden = true;
                    statusNode.dataset.tone = "";
                    statusNode.textContent = "";
                }, 1700);
            }
        };

        tableWrap.querySelectorAll("[data-note-control='select']").forEach((select) => {
            select.dataset.previousValue = select.value;
        });
        tableWrap.querySelectorAll("[data-note-control='input'], [data-note-control='text']").forEach((input) => {
            input.dataset.previousValue = input.value;
        });

        const commitInput = async (input) => {
            if (input.classList.contains("is-pending")) {
                return;
            }
            const previous = input.dataset.previousValue ?? "";
            const next = input.value;
            if (next === previous) {
                return;
            }
            const keywordField = (input.dataset.field || "").startsWith("keyword_");
            const payload = {
                product_id: input.dataset.productId,
                note_date: input.dataset.noteDate,
                field: input.dataset.field,
                value: next,
            };
            if (typeof input.dataset.keywordPrev !== "undefined") {
                payload.keyword_prev = input.dataset.keywordPrev || "";
            }
            if (keywordField) {
                const keywordEntry = input.closest("[data-keyword-entry]");
                const queryInput = keywordEntry
                    ? keywordEntry.querySelector("[data-field='keyword_query']")
                    : null;
                payload.keyword_query = queryInput ? queryInput.value.trim() : "";
            }
            try {
                setInputPending(input, true);
                const result = await saveCell(updateUrl, payload);
                const serverValue = result && typeof result.value !== "undefined" ? String(result.value ?? "") : next;
                input.value = serverValue;
                input.dataset.previousValue = serverValue;
                if (typeof input.dataset.keywordPrev !== "undefined") {
                    input.dataset.keywordPrev = serverValue;
                }
                if (input.dataset.field === "keyword_query") {
                    const keywordEntry = input.closest("[data-keyword-entry]");
                    (keywordEntry ? keywordEntry.querySelectorAll("[data-note-control='input'][data-keyword-prev]") : [])
                        .forEach((relatedInput) => {
                            relatedInput.dataset.keywordPrev = serverValue;
                        });
                }
                showStatus("Сохранено", "success");
                const cell = input.closest("td[data-block]");
                if (cell && typeof tableWrap._recalculateBlock === "function") {
                    const blockIndex = Number.parseInt(cell.dataset.block || "", 10);
                    if (Number.isFinite(blockIndex)) {
                        tableWrap._recalculateBlock(blockIndex);
                    }
                }
            } catch (error) {
                input.value = previous;
                showStatus(`Ошибка сохранения: ${error.message}`, "error");
            } finally {
                setInputPending(input, false);
            }
        };

        tableWrap.addEventListener("click", async (event) => {
            const keywordRowsButton = event.target.closest(".grid-keyword-action");
            if (keywordRowsButton) {
                const wrapper = keywordRowsButton.closest("[data-note-control='keyword-rows']");
                if (!wrapper || wrapper.classList.contains("is-pending")) {
                    return;
                }
                const payload = {
                    product_id: wrapper.dataset.productId,
                    note_date: wrapper.dataset.noteDate,
                    field: wrapper.dataset.field,
                    value: keywordRowsButton.dataset.delta,
                };
                try {
                    setKeywordRowsPending(wrapper, true);
                    await saveCell(updateUrl, payload);
                    window.location.reload();
                } catch (error) {
                    showStatus(`Ошибка сохранения: ${error.message}`, "error");
                    setKeywordRowsPending(wrapper, false);
                }
                return;
            }

            const button = event.target.closest(".grid-choice-btn");
            if (!button) {
                return;
            }
            const wrapper = button.closest("[data-note-control='bool']");
            if (!wrapper || wrapper.classList.contains("is-pending")) {
                return;
            }
            const activeButton = wrapper.querySelector(".grid-choice-btn.is-active");
            const previousValue = activeButton ? activeButton.dataset.value === "true" : false;
            const payload = {
                product_id: wrapper.dataset.productId,
                note_date: wrapper.dataset.noteDate,
                field: wrapper.dataset.field,
                value: button.dataset.value === "true",
            };
            if (payload.value === previousValue) {
                return;
            }
            try {
                setBoolPending(wrapper, true);
                await saveCell(updateUrl, payload);
                setBoolActive(wrapper, payload.value);
                showStatus("Сохранено", "success");
            } catch (error) {
                setBoolActive(wrapper, previousValue);
                showStatus(`Ошибка сохранения: ${error.message}`, "error");
            } finally {
                setBoolPending(wrapper, false);
            }
        });

        tableWrap.addEventListener("change", async (event) => {
            const select = event.target.closest("[data-note-control='select']");
            if (select && !select.classList.contains("is-pending")) {
                const previous = select.dataset.previousValue ?? select.value;
                const payload = {
                    product_id: select.dataset.productId,
                    note_date: select.dataset.noteDate,
                    field: select.dataset.field,
                    value: select.value,
                };
                try {
                    setSelectPending(select, true);
                    const result = await saveCell(updateUrl, payload);
                    const serverValue = result && typeof result.value !== "undefined" ? String(result.value ?? "") : select.value;
                    select.dataset.previousValue = serverValue;
                    showStatus("Сохранено", "success");
                } catch (error) {
                    select.value = previous;
                    showStatus(`Ошибка сохранения: ${error.message}`, "error");
                } finally {
                    setSelectPending(select, false);
                }
                return;
            }

            const input = event.target.closest("[data-note-control='input'], [data-note-control='text']");
            if (!input) {
                return;
            }
            await commitInput(input);
        });

        tableWrap.addEventListener("input", (event) => {
            const input = event.target.closest("[data-note-control='input']");
            if (!input) {
                return;
            }
            const cell = input.closest("td[data-block]");
            if (cell && typeof tableWrap._recalculateBlock === "function") {
                const blockIndex = Number.parseInt(cell.dataset.block || "", 10);
                if (Number.isFinite(blockIndex)) {
                    tableWrap._recalculateBlock(blockIndex);
                }
            }
        });

        tableWrap.addEventListener("keydown", (event) => {
            const input = event.target.closest("[data-note-control='input']");
            if (!input) {
                return;
            }
            if (event.key === "Enter") {
                event.preventDefault();
                input.blur();
            }
        });

        tableWrap.addEventListener(
            "blur",
            async (event) => {
                const input = event.target.closest("[data-note-control='input'], [data-note-control='text']");
                if (!input) {
                    return;
                }
                await commitInput(input);
            },
            true
        );
    }

    function initDensityToggle(tableWrap) {
        const button = document.querySelector("[data-table-action='toggle-density']");
        if (!button) {
            return;
        }
        const defaultCompact = (tableWrap.dataset.defaultDensity || "compact") === "compact";
        const apply = (compact) => {
            tableWrap.classList.toggle("is-compact", compact);
            button.textContent = compact ? "Обычный режим" : "Компактный режим";
        };
        apply(defaultCompact);
        button.addEventListener("click", () => {
            const next = !tableWrap.classList.contains("is-compact");
            apply(next);
        });
    }

    function initFullscreenToggle(tableWrap) {
        const button = document.querySelector("[data-table-action='fullscreen']");
        if (!button) {
            return;
        }
        const defaultFullscreen = (tableWrap.dataset.defaultFullscreen || "normal") === "fullscreen";
        const apply = (enabled) => {
            document.body.classList.toggle("is-table-fullscreen", enabled);
            button.textContent = enabled ? "Выйти из полноэкранного" : "Режим на весь экран";
            button.setAttribute("aria-pressed", enabled ? "true" : "false");
            if (enabled) {
                tableWrap.scrollIntoView({ block: "start", inline: "nearest" });
            }
        };
        apply(defaultFullscreen);
        button.addEventListener("click", () => {
            const next = !document.body.classList.contains("is-table-fullscreen");
            apply(next);
        });
        window.addEventListener("keydown", (event) => {
            if (event.key === "Escape" && document.body.classList.contains("is-table-fullscreen")) {
                apply(false);
            }
        });
    }

    function initActionModals() {
        const modals = Array.from(document.querySelectorAll("[data-table-modal]"));
        if (!modals.length) {
            return;
        }
        const stocksModal = document.getElementById("table-modal-stocks");
        const stocksModalMeta = stocksModal ? stocksModal.querySelector("[data-stocks-modal-meta]") : null;
        const stocksModalBody = stocksModal ? stocksModal.querySelector("[data-stocks-modal-body]") : null;

        const focusByModal = new WeakMap();
        const openedModals = () => modals.filter((modal) => !modal.hidden);
        const syncBodyState = () => {
            document.body.classList.toggle("is-modal-open", openedModals().length > 0);
        };

        const parseStockPayload = (rawPayload) => {
            const fallback = {
                mode: "flat",
                columns: [
                    { id: "warehouse", label: "Склад", numeric: false, blank_zero: false },
                    { id: "stock", label: "Остаток", numeric: true, blank_zero: false },
                ],
                rows: [],
                empty_message: "Нет данных по складам для выбранной даты.",
            };
            if (!rawPayload) {
                return fallback;
            }
            try {
                const parsed = JSON.parse(rawPayload);
                if (!parsed || !Array.isArray(parsed.columns) || !Array.isArray(parsed.rows)) {
                    return fallback;
                }
                const columns = parsed.columns
                    .map((column) => {
                        const id = String(column && column.id ? column.id : "").trim();
                        const label = String(column && column.label ? column.label : "").trim();
                        if (!id || !label) {
                            return null;
                        }
                        return {
                            id,
                            label,
                            numeric: Boolean(column && column.numeric),
                            blank_zero: Boolean(column && column.blank_zero),
                        };
                    })
                    .filter((column) => Boolean(column));
                if (!columns.length) {
                    return fallback;
                }
                return {
                    mode: parsed.mode === "matrix" ? "matrix" : "flat",
                    columns,
                    rows: parsed.rows.filter((row) => row && typeof row === "object"),
                    empty_message: String(parsed.empty_message || fallback.empty_message),
                };
            } catch (error) {
                return fallback;
            }
        };

        const renderStocksModalHead = (headNode, columns) => {
            if (!headNode) {
                return;
            }
            headNode.innerHTML = "";
            const row = document.createElement("tr");
            columns.forEach((column) => {
                const cell = document.createElement("th");
                cell.textContent = column.label;
                if (column.numeric) {
                    cell.className = "is-numeric";
                }
                row.appendChild(cell);
            });
            headNode.appendChild(row);
        };

        let stocksRequestToken = 0;

        const renderStocksModalMessage = (message) => {
            const stocksModalHead = stocksModal ? stocksModal.querySelector("[data-stocks-modal-head]") : null;
            if (!stocksModalHead || !stocksModalBody) {
                return;
            }
            stocksModalHead.innerHTML = "";
            stocksModalBody.innerHTML = "";
            const row = document.createElement("tr");
            const cell = document.createElement("td");
            cell.className = "table-stocks-modal-empty";
            cell.textContent = message;
            row.appendChild(cell);
            stocksModalBody.appendChild(row);
        };

        const loadStocksModalPayload = async (trigger) => {
            const stockUrl = String(trigger.getAttribute("data-stock-url") || "").trim();
            if (!stockUrl) {
                return {
                    date_label: String(trigger.getAttribute("data-stock-date") || "").trim(),
                    total: 0,
                    payload: parseStockPayload(""),
                };
            }
            const response = await fetch(stockUrl, {
                headers: { Accept: "application/json" },
                cache: "no-store",
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok || !data.ok || !data.payload) {
                const detail = data && data.detail ? data.detail : `HTTP ${response.status}`;
                throw new Error(detail);
            }
            return {
                date_label: String(data.date_label || trigger.getAttribute("data-stock-date") || "").trim(),
                total: Number.parseInt(String(data.total || "0"), 10),
                payload: parseStockPayload(JSON.stringify(data.payload)),
            };
        };

        const hydrateStocksModal = async (trigger) => {
            const stocksModalHead = stocksModal ? stocksModal.querySelector("[data-stocks-modal-head]") : null;
            if (!stocksModal || !stocksModalMeta || !stocksModalBody || !stocksModalHead || !trigger) {
                return;
            }
            const dateLabel = String(trigger.getAttribute("data-stock-date") || "").trim();
            stocksModalMeta.textContent = dateLabel ? `Срез на ${dateLabel}. Загружаем данные...` : "Загружаем данные...";
            renderStocksModalMessage("Загружаем данные по складам...");

            const requestToken = stocksRequestToken + 1;
            stocksRequestToken = requestToken;

            try {
                const payloadResult = await loadStocksModalPayload(trigger);
                if (requestToken !== stocksRequestToken) {
                    return;
                }
                const payload = payloadResult.payload;
                const totalLabel = Number.isFinite(payloadResult.total)
                    ? payloadResult.total.toLocaleString("ru-RU")
                    : "0";
                stocksModalMeta.textContent = payloadResult.date_label
                    ? `Срез на ${payloadResult.date_label}. Итого по складам: ${totalLabel} шт.`
                    : `Итого по складам: ${totalLabel} шт.`;
                stocksModalBody.innerHTML = "";
                renderStocksModalHead(stocksModalHead, payload.columns);

                if (!payload.rows.length) {
                    const emptyRow = document.createElement("tr");
                    const emptyCell = document.createElement("td");
                    emptyCell.colSpan = payload.columns.length;
                    emptyCell.className = "table-stocks-modal-empty";
                    emptyCell.textContent = payload.empty_message;
                    emptyRow.appendChild(emptyCell);
                    stocksModalBody.appendChild(emptyRow);
                    return;
                }

                payload.rows.forEach((row) => {
                    const tableRow = document.createElement("tr");
                    payload.columns.forEach((column) => {
                        const cell = document.createElement("td");
                        const rawValue = row[column.id];
                        if (column.numeric) {
                            const numericValue = Number.parseInt(String(rawValue ?? ""), 10);
                            cell.className = "is-numeric";
                            if (Number.isFinite(numericValue) && (!column.blank_zero || numericValue !== 0)) {
                                cell.textContent = numericValue.toLocaleString("ru-RU");
                            } else {
                                cell.textContent = "";
                            }
                        } else {
                            cell.textContent = String(rawValue ?? "").trim();
                        }
                        tableRow.appendChild(cell);
                    });
                    stocksModalBody.appendChild(tableRow);
                });
            } catch (error) {
                if (requestToken !== stocksRequestToken) {
                    return;
                }
                stocksModalMeta.textContent = dateLabel
                    ? `Срез на ${dateLabel}. Не удалось загрузить данные.`
                    : "Не удалось загрузить данные по складам.";
                renderStocksModalMessage("Не удалось загрузить данные по складам.");
            }
        };

        const openModal = (modal, trigger) => {
            if (!modal || !modal.hidden) {
                return;
            }
            focusByModal.set(modal, trigger || document.activeElement);
            modal.hidden = false;
            syncBodyState();
            const firstFocusable = modal.querySelector(
                "[autofocus], input, select, textarea, button, a[href], [tabindex]:not([tabindex='-1'])"
            );
            if (firstFocusable && typeof firstFocusable.focus === "function") {
                firstFocusable.focus();
            }
        };

        const closeModal = (modal) => {
            if (!modal || modal.hidden) {
                return;
            }
            modal.hidden = true;
            const focusTarget = focusByModal.get(modal);
            if (focusTarget && typeof focusTarget.focus === "function") {
                focusTarget.focus();
            }
            syncBodyState();
        };

        document.addEventListener("click", (event) => {
            const openTrigger = event.target.closest("[data-modal-open]");
            if (openTrigger) {
                const targetId = openTrigger.dataset.modalOpen;
                const modal = targetId ? document.getElementById(targetId) : null;
                if (!modal || !modal.matches("[data-table-modal]")) {
                    return;
                }
                event.preventDefault();
                openModal(modal, openTrigger);
                if (openTrigger.matches("[data-stock-popup-button]")) {
                    void hydrateStocksModal(openTrigger);
                }
                return;
            }

            const closeTrigger = event.target.closest("[data-modal-close]");
            if (closeTrigger) {
                const modal = closeTrigger.closest("[data-table-modal]");
                if (!modal) {
                    return;
                }
                event.preventDefault();
                closeModal(modal);
            }
        });

        document.addEventListener("keydown", (event) => {
            if (event.key !== "Escape") {
                return;
            }
            const activeModal = openedModals().pop();
            if (!activeModal) {
                return;
            }
            event.preventDefault();
            closeModal(activeModal);
        });
    }

    function initToolbarFilters() {
        const form = document.querySelector("[data-table-filter-form]");
        if (!form) {
            return;
        }
        const referenceInput = form.querySelector("input[name='reference_date']");
        const historyInput = form.querySelector("input[name='history_days']");
        const picker = form.querySelector("[data-table-period-picker]");
        if (!referenceInput || !historyInput || !picker) {
            return;
        }
        const trigger = picker.querySelector("[data-period-trigger]");
        const triggerLabel = picker.querySelector("[data-period-trigger-label]");
        const panel = picker.querySelector("[data-period-panel]");
        const monthsHead = picker.querySelector("[data-period-months]");
        const calendarsRoot = picker.querySelector("[data-period-calendars]");
        const summaryLabel = picker.querySelector("[data-period-summary-label]");
        const summaryMeta = picker.querySelector("[data-period-summary-meta]");
        const applyButton = picker.querySelector("[data-period-apply]");
        const cancelButton = picker.querySelector("[data-period-cancel]");
        const customButton = picker.querySelector("[data-period-custom]");
        const navButtons = Array.from(picker.querySelectorAll("[data-period-nav]"));
        const presetButtons = Array.from(picker.querySelectorAll("[data-period-preset]"));
        const maxDays = Math.max(1, Math.min(90, Number.parseInt(picker.dataset.maxDays || "90", 10) || 90));

        const monthFormatter = new Intl.DateTimeFormat("ru-RU", { month: "long", year: "numeric" });
        const triggerFormatter = new Intl.DateTimeFormat("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" });
        const weekdayLabels = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"];

        const parseIsoDate = (rawValue) => {
            const match = String(rawValue || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
            if (!match) {
                return null;
            }
            return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
        };

        const formatIsoDate = (dateValue) => {
            const year = dateValue.getFullYear();
            const month = String(dateValue.getMonth() + 1).padStart(2, "0");
            const day = String(dateValue.getDate()).padStart(2, "0");
            return `${year}-${month}-${day}`;
        };

        const formatTriggerRange = (startDate, endDate) =>
            `${triggerFormatter.format(startDate)} — ${triggerFormatter.format(endDate)}`;

        const stripTime = (dateValue) => new Date(dateValue.getFullYear(), dateValue.getMonth(), dateValue.getDate());
        const addDays = (dateValue, days) => {
            const next = new Date(dateValue);
            next.setDate(next.getDate() + days);
            return stripTime(next);
        };
        const addMonths = (dateValue, months) => new Date(dateValue.getFullYear(), dateValue.getMonth() + months, 1);
        const startOfMonth = (dateValue) => new Date(dateValue.getFullYear(), dateValue.getMonth(), 1);
        const endOfMonth = (dateValue) => new Date(dateValue.getFullYear(), dateValue.getMonth() + 1, 0);
        const isSameDay = (left, right) =>
            left &&
            right &&
            left.getFullYear() === right.getFullYear() &&
            left.getMonth() === right.getMonth() &&
            left.getDate() === right.getDate();
        const diffDaysInclusive = (startDate, endDate) =>
            Math.round((stripTime(endDate).getTime() - stripTime(startDate).getTime()) / 86400000) + 1;
        const rangeLength = (startDate, endDate) => diffDaysInclusive(startDate, endDate);

        const fallbackEnd = parseIsoDate(referenceInput.value) || stripTime(new Date());
        const fallbackDays = Math.max(1, Math.min(maxDays, Number.parseInt(historyInput.value || "14", 10) || 14));
        const appliedEnd = parseIsoDate(picker.dataset.currentEnd) || fallbackEnd;
        const appliedStart = parseIsoDate(picker.dataset.currentStart) || addDays(appliedEnd, -(fallbackDays - 1));

        const state = {
            appliedStart: stripTime(appliedStart),
            appliedEnd: stripTime(appliedEnd),
            appliedIsCustom: false,
            draftStart: stripTime(appliedStart),
            draftEnd: stripTime(appliedEnd),
            draftIsCustom: false,
            visibleMonth: startOfMonth(appliedStart),
            selectingRange: false,
        };

        const syncTrigger = () => {
            if (triggerLabel) {
                triggerLabel.textContent = formatTriggerRange(state.appliedStart, state.appliedEnd);
            }
        };

        const syncSummary = () => {
            const days = rangeLength(state.draftStart, state.draftEnd);
            if (summaryLabel) {
                summaryLabel.textContent = formatTriggerRange(state.draftStart, state.draftEnd);
            }
            if (summaryMeta) {
                summaryMeta.textContent = days > maxDays ? `Максимум ${maxDays} дн.` : `${days} дн.`;
            }
            if (applyButton) {
                applyButton.disabled = days > maxDays;
            }
        };

        const syncPresets = () => {
            const days = rangeLength(state.draftStart, state.draftEnd);
            let matched = false;
            presetButtons.forEach((button) => {
                const presetDays = Number.parseInt(button.dataset.periodPreset || "", 10);
                const isCustom = button.hasAttribute("data-period-custom");
                const isActive = !isCustom && !state.draftIsCustom && Number.isFinite(presetDays) && presetDays === days;
                button.classList.toggle("is-active", isActive);
                matched = matched || isActive;
            });
            if (customButton) {
                customButton.classList.toggle("is-active", state.draftIsCustom || !matched);
            }
        };

        const renderCalendars = () => {
            if (!monthsHead || !calendarsRoot) {
                return;
            }
            monthsHead.innerHTML = "";
            calendarsRoot.innerHTML = "";

            [state.visibleMonth, addMonths(state.visibleMonth, 1)].forEach((monthDate) => {
                const monthTitle = document.createElement("div");
                monthTitle.className = "table-period-month-heading";
                monthTitle.textContent = monthFormatter.format(monthDate);
                monthsHead.appendChild(monthTitle);

                const month = document.createElement("section");
                month.className = "table-period-month";

                const weekdays = document.createElement("div");
                weekdays.className = "table-period-weekdays";
                weekdayLabels.forEach((label) => {
                    const node = document.createElement("span");
                    node.textContent = label;
                    weekdays.appendChild(node);
                });
                month.appendChild(weekdays);

                const daysGrid = document.createElement("div");
                daysGrid.className = "table-period-days";
                const monthStart = startOfMonth(monthDate);
                const monthEnd = endOfMonth(monthDate);
                const startOffset = (monthStart.getDay() || 7) - 1;
                const daysInMonth = monthEnd.getDate();
                const totalCells = Math.ceil((startOffset + daysInMonth) / 7) * 7;

                for (let cellIndex = 0; cellIndex < totalCells; cellIndex += 1) {
                    const dayNumber = cellIndex - startOffset + 1;
                    if (dayNumber < 1 || dayNumber > daysInMonth) {
                        const placeholder = document.createElement("span");
                        placeholder.className = "table-period-day is-placeholder";
                        placeholder.setAttribute("aria-hidden", "true");
                        daysGrid.appendChild(placeholder);
                        continue;
                    }

                    const cursor = new Date(monthDate.getFullYear(), monthDate.getMonth(), dayNumber);
                    const dayButton = document.createElement("button");
                    dayButton.type = "button";
                    dayButton.className = "table-period-day";
                    dayButton.textContent = String(cursor.getDate());
                    dayButton.dataset.date = formatIsoDate(cursor);

                    const isStart = isSameDay(cursor, state.draftStart);
                    const isEnd = isSameDay(cursor, state.draftEnd);
                    const inRange = cursor >= state.draftStart && cursor <= state.draftEnd;
                    if (inRange) {
                        dayButton.classList.add("is-in-range");
                    }
                    if (isStart && isEnd) {
                        dayButton.classList.add("is-single");
                    } else {
                        if (isStart) {
                            dayButton.classList.add("is-range-start");
                        }
                        if (isEnd) {
                            dayButton.classList.add("is-range-end");
                        }
                    }

                    dayButton.addEventListener("click", () => {
                        const picked = parseIsoDate(dayButton.dataset.date);
                        if (!picked) {
                            return;
                        }
                        state.draftIsCustom = true;
                        if (!state.selectingRange) {
                            state.draftStart = picked;
                            state.draftEnd = picked;
                            state.selectingRange = true;
                        } else {
                            if (picked < state.draftStart) {
                                state.draftEnd = state.draftStart;
                                state.draftStart = picked;
                            } else {
                                state.draftEnd = picked;
                            }
                            state.selectingRange = false;
                        }
                        syncSummary();
                        syncPresets();
                        renderCalendars();
                    });

                    daysGrid.appendChild(dayButton);
                }
                month.appendChild(daysGrid);
                calendarsRoot.appendChild(month);
            });
        };

        const openPanel = () => {
            state.draftStart = stripTime(state.appliedStart);
            state.draftEnd = stripTime(state.appliedEnd);
            state.draftIsCustom = state.appliedIsCustom;
            state.visibleMonth = startOfMonth(state.draftStart);
            state.selectingRange = false;
            picker.classList.add("is-open");
            panel.hidden = false;
            trigger.setAttribute("aria-expanded", "true");
            syncSummary();
            syncPresets();
            renderCalendars();
        };

        const closePanel = () => {
            picker.classList.remove("is-open");
            panel.hidden = true;
            trigger.setAttribute("aria-expanded", "false");
            state.selectingRange = false;
        };

        syncTrigger();
        syncSummary();
        syncPresets();

        trigger.addEventListener("click", () => {
            if (picker.classList.contains("is-open")) {
                closePanel();
                return;
            }
            openPanel();
        });

        picker.addEventListener("click", (event) => {
            event.stopPropagation();
        });

        navButtons.forEach((button) => {
            button.addEventListener("click", () => {
                const direction = Number.parseInt(button.dataset.periodNav || "0", 10);
                if (!Number.isFinite(direction) || direction === 0) {
                    return;
                }
                state.visibleMonth = addMonths(state.visibleMonth, direction);
                renderCalendars();
            });
        });

        presetButtons.forEach((button) => {
            if (!button.dataset.periodPreset) {
                return;
            }
            button.addEventListener("click", () => {
                const days = Number.parseInt(button.dataset.periodPreset || "0", 10);
                if (!Number.isFinite(days) || days <= 0) {
                    return;
                }
                state.draftEnd = stripTime(state.draftEnd);
                state.draftStart = addDays(state.draftEnd, -(days - 1));
                state.visibleMonth = startOfMonth(state.draftStart);
                state.selectingRange = false;
                syncSummary();
                syncPresets();
                renderCalendars();
            });
        });

        if (customButton) {
            customButton.addEventListener("click", () => {
                state.draftIsCustom = true;
                state.draftStart = stripTime(state.draftEnd);
                state.draftEnd = stripTime(state.draftEnd);
                state.selectingRange = false;
                state.visibleMonth = startOfMonth(state.draftEnd);
                syncSummary();
                syncPresets();
                renderCalendars();
            });
        }

        if (cancelButton) {
            cancelButton.addEventListener("click", () => {
                closePanel();
            });
        }

        if (applyButton) {
            applyButton.addEventListener("click", () => {
                const days = rangeLength(state.draftStart, state.draftEnd);
                if (days > maxDays) {
                    syncSummary();
                    return;
                }
                state.appliedStart = stripTime(state.draftStart);
                state.appliedEnd = stripTime(state.draftEnd);
                state.appliedIsCustom = state.draftIsCustom;
                referenceInput.value = formatIsoDate(state.appliedEnd);
                historyInput.value = String(days);
                syncTrigger();
                closePanel();
                form.requestSubmit();
            });
        }

        document.addEventListener("click", (event) => {
            if (!picker.classList.contains("is-open")) {
                return;
            }
            if (!picker.contains(event.target)) {
                closePanel();
            }
        });

        document.addEventListener("keydown", (event) => {
            if (event.key === "Escape" && picker.classList.contains("is-open")) {
                closePanel();
            }
        });
    }

    function initSyncQuickRanges() {
        const modal = document.getElementById("table-modal-sync");
        if (!modal) {
            return;
        }
        const form = modal.querySelector("form");
        if (!form) {
            return;
        }
        const dateFrom = form.querySelector("input[name='date_from']");
        const dateTo = form.querySelector("input[name='date_to']");
        const reference = form.querySelector("input[name='reference_date']");
        const buttons = Array.from(modal.querySelectorAll("[data-sync-range]"));
        if (!buttons.length || (!dateFrom && !dateTo && !reference)) {
            return;
        }

        const formatDate = (dateValue) => {
            const year = dateValue.getFullYear();
            const month = String(dateValue.getMonth() + 1).padStart(2, "0");
            const day = String(dateValue.getDate()).padStart(2, "0");
            return `${year}-${month}-${day}`;
        };

        const applyRange = (offsetDays) => {
            const target = new Date();
            target.setDate(target.getDate() + offsetDays);
            const value = formatDate(target);
            if (dateFrom) {
                dateFrom.value = value;
            }
            if (dateTo) {
                dateTo.value = value;
            }
            if (reference) {
                reference.value = value;
            }
            buttons.forEach((button) => {
                button.classList.toggle("is-active", button.dataset.syncRange === (offsetDays === 0 ? "today" : "yesterday"));
            });
        };

        buttons.forEach((button) => {
            button.addEventListener("click", () => {
                const range = String(button.dataset.syncRange || "").toLowerCase();
                if (range === "today") {
                    applyRange(0);
                    return;
                }
                if (range === "yesterday") {
                    applyRange(-1);
                }
            });
        });
    }

    function initSyncProductPicker() {
        const selects = Array.from(document.querySelectorAll("select[data-sync-product-select]"));
        if (!selects.length) {
            return;
        }

        const normalize = (value) => String(value || "").trim().toLowerCase();

        selects.forEach((select) => {
            if (select.dataset.enhanced === "1") {
                return;
            }
            select.dataset.enhanced = "1";
            select.classList.add("is-enhanced");

            const field = select.closest(".field") || select.parentElement;
            if (!field) {
                return;
            }

            const wrapper = document.createElement("div");
            wrapper.className = "sync-product-picker";

            const header = document.createElement("div");
            header.className = "sync-product-picker-head";
            const title = document.createElement("span");
            title.className = "sync-product-picker-title";
            title.textContent = "Выбор товаров";
            const count = document.createElement("span");
            count.className = "sync-product-picker-count";
            header.append(title, count);

            const search = document.createElement("input");
            search.type = "search";
            search.className = "sync-product-search";
            search.placeholder = "Поиск товара по названию или nmID";

            const actions = document.createElement("div");
            actions.className = "sync-product-actions";
            const selectAll = document.createElement("button");
            selectAll.type = "button";
            selectAll.className = "secondary-button sync-product-action";
            selectAll.textContent = "Выбрать все";
            const clearAll = document.createElement("button");
            clearAll.type = "button";
            clearAll.className = "secondary-button sync-product-action";
            clearAll.textContent = "Очистить";
            actions.append(selectAll, clearAll);

            const list = document.createElement("div");
            list.className = "sync-product-list";
            const empty = document.createElement("div");
            empty.className = "sync-product-empty";
            empty.textContent = "Нет совпадений.";

            const selected = document.createElement("div");
            selected.className = "sync-product-selected";
            const selectedLabel = document.createElement("span");
            selectedLabel.className = "sync-product-selected-label";
            selectedLabel.textContent = "Выбрано:";
            const chips = document.createElement("div");
            chips.className = "sync-product-chips";
            selected.append(selectedLabel, chips);

            wrapper.append(header, search, actions, list, empty, selected);
            field.append(wrapper);

            const options = Array.from(select.options).filter((option) => option.value !== "");
            const rows = options.map((option) => {
                const row = document.createElement("label");
                row.className = "sync-product-row";
                row.dataset.search = normalize(`${option.text} ${option.value}`);

                const checkbox = document.createElement("input");
                checkbox.type = "checkbox";
                checkbox.value = option.value;
                checkbox.checked = option.selected;
                checkbox.disabled = option.disabled;

                const text = document.createElement("span");
                text.className = "sync-product-row-label";
                text.textContent = option.text;

                row.append(checkbox, text);
                list.append(row);
                return { row, checkbox, option };
            });

            const syncSelected = () => {
                const selectedOptions = options.filter((option) => option.selected);
                count.textContent = selectedOptions.length ? `Выбрано: ${selectedOptions.length}` : "Ничего не выбрано";
                chips.innerHTML = "";
                if (!selectedOptions.length) {
                    selected.classList.add("is-empty");
                    return;
                }
                selected.classList.remove("is-empty");
                selectedOptions.forEach((option) => {
                    const chip = document.createElement("button");
                    chip.type = "button";
                    chip.className = "sync-product-chip";
                    chip.dataset.value = option.value;
                    chip.innerHTML = `${option.text}<span aria-hidden=\"true\">×</span>`;
                    chip.addEventListener("click", () => {
                        option.selected = false;
                        const targetRow = rows.find((item) => item.option.value === option.value);
                        if (targetRow) {
                            targetRow.checkbox.checked = false;
                        }
                        syncSelected();
                    });
                    chips.append(chip);
                });
            };

            const applyFilter = () => {
                const query = normalize(search.value);
                let matches = 0;
                rows.forEach(({ row }) => {
                    const hit = !query || row.dataset.search.includes(query);
                    row.hidden = !hit;
                    if (hit) {
                        matches += 1;
                    }
                });
                empty.hidden = matches > 0;
            };

            rows.forEach(({ checkbox, option }) => {
                checkbox.addEventListener("change", () => {
                    option.selected = checkbox.checked;
                    syncSelected();
                });
            });

            selectAll.addEventListener("click", () => {
                options.forEach((option) => {
                    if (option.disabled) {
                        return;
                    }
                    option.selected = true;
                });
                rows.forEach(({ checkbox, option }) => {
                    if (!option.disabled) {
                        checkbox.checked = true;
                    }
                });
                syncSelected();
            });

            clearAll.addEventListener("click", () => {
                options.forEach((option) => {
                    option.selected = false;
                });
                rows.forEach(({ checkbox }) => {
                    checkbox.checked = false;
                });
                syncSelected();
            });

            search.addEventListener("input", applyFilter);

            syncSelected();
            applyFilter();
        });
    }

    function initSheetsNavigation() {
        const nav = document.querySelector("[data-sheets-nav]");
        if (!nav) {
            return;
        }
        const track = nav.querySelector("[data-sheets-track]");
        if (!track) {
            return;
        }
        const prevButton = nav.querySelector("[data-sheets-step='-1']");
        const nextButton = nav.querySelector("[data-sheets-step='1']");

        const maxScrollLeft = () => Math.max(0, track.scrollWidth - track.clientWidth);
        const updateButtons = () => {
            const max = maxScrollLeft();
            if (prevButton) {
                prevButton.disabled = track.scrollLeft <= 1;
            }
            if (nextButton) {
                nextButton.disabled = track.scrollLeft >= max - 1;
            }
        };

        const scrollTrack = (delta) => {
            const step = Math.max(120, Math.round(track.clientWidth * 0.75));
            track.scrollBy({ left: step * delta, behavior: "smooth" });
        };

        if (prevButton) {
            prevButton.addEventListener("click", () => scrollTrack(-1));
        }
        if (nextButton) {
            nextButton.addEventListener("click", () => scrollTrack(1));
        }

        let scrollRaf = 0;
        track.addEventListener("scroll", () => {
            if (scrollRaf) {
                return;
            }
            scrollRaf = window.requestAnimationFrame(() => {
                scrollRaf = 0;
                updateButtons();
            });
        });

        const activeTab = track.querySelector(".segmented-button.is-active");
        if (activeTab) {
            activeTab.scrollIntoView({ block: "nearest", inline: "center" });
        }
        updateButtons();
    }

    function initDayNavigation(tableWrap) {
        const nav = document.querySelector("[data-day-nav]");
        if (!nav) {
            return;
        }
        const track = nav.querySelector("[data-day-track]") || nav;
        const firstRow = tableWrap.querySelector("tbody tr");
        if (!firstRow) {
            return;
        }
        const chips = Array.from(track.querySelectorAll("[data-day-index]"));
        if (!chips.length) {
            return;
        }
        const prevButton = nav.querySelector("[data-day-step='-1']");
        const nextButton = nav.querySelector("[data-day-step='1']");
        const meta = document.querySelector("[data-day-meta]");
        const rawDefaultIndex = Number.parseInt(nav.dataset.defaultDayIndex || "", 10);

        const dateAnchors = Array.from(firstRow.querySelectorAll("td")).filter((cell) =>
            /^\d{2}\.\d{2}\.\d{4}$/.test((cell.textContent || "").trim())
        );
        const blockStartCells = Array.from(firstRow.querySelectorAll("td.is-block-start"));
        const anchors =
            dateAnchors.length >= chips.length
                ? dateAnchors.slice(0, chips.length)
                : blockStartCells.filter((cell) => cell.offsetWidth > 0);
        if (anchors.length < chips.length) {
            return;
        }

        const stickyLabel = firstRow.querySelector("td.is-label-col");
        const stickySpacer = firstRow.querySelector("td.is-spacer-col");
        const getStickyOffset = () =>
            (stickyLabel ? stickyLabel.getBoundingClientRect().width : 0) +
            (stickySpacer ? stickySpacer.getBoundingClientRect().width : 0);
        const getAnchorLeft = (cell) => {
            const wrapRect = tableWrap.getBoundingClientRect();
            const cellRect = cell.getBoundingClientRect();
            return cellRect.left - wrapRect.left + tableWrap.scrollLeft;
        };

        const applyActive = (index, options = {}) => {
            const syncChip = Boolean(options.syncChip);
            chips.forEach((chip, chipIndex) => {
                chip.classList.toggle("is-current", chipIndex === index);
                chip.setAttribute("aria-pressed", chipIndex === index ? "true" : "false");
            });
            if (meta) {
                const activeChip = chips[index];
                const activeDate = activeChip ? (activeChip.textContent || "").trim() : "";
                meta.textContent = activeDate ? `Текущий день: ${activeDate} (${index + 1}/${chips.length})` : "";
            }
            if (syncChip && chips[index]) {
                chips[index].scrollIntoView({ block: "nearest", inline: "center", behavior: "smooth" });
            }
            if (prevButton) {
                prevButton.disabled = index <= 0;
            }
            if (nextButton) {
                nextButton.disabled = index >= chips.length - 1;
            }
        };

        const scrollToDayIndex = (index, behavior = "smooth") => {
            const target = anchors[index];
            if (!target) {
                return;
            }
            const left = Math.max(0, getAnchorLeft(target) - getStickyOffset() - 6);
            tableWrap.scrollTo({ left, behavior });
            applyActive(index, { syncChip: true });
        };

        const resolveCurrentDayIndex = () => {
            const probe = tableWrap.scrollLeft + getStickyOffset() + 6;
            let nearestIndex = 0;
            let nearestDistance = Number.POSITIVE_INFINITY;
            anchors.forEach((cell, index) => {
                const distance = Math.abs(getAnchorLeft(cell) - probe);
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    nearestIndex = index;
                }
            });
            return nearestIndex;
        };

        const defaultIndex = Number.isFinite(rawDefaultIndex)
            ? Math.max(0, Math.min(chips.length - 1, rawDefaultIndex))
            : chips.length > 1
                ? chips.length - 2
                : 0;

        chips.forEach((chip) => {
            chip.addEventListener("click", () => {
                const index = Number(chip.dataset.dayIndex || "0");
                scrollToDayIndex(index);
            });
        });

        const shiftDay = (delta) => {
            const current = resolveCurrentDayIndex();
            const next = Math.max(0, Math.min(chips.length - 1, current + delta));
            if (next !== current) {
                scrollToDayIndex(next);
                return;
            }
            applyActive(current, { syncChip: true });
        };

        if (prevButton) {
            prevButton.addEventListener("click", () => shiftDay(-1));
        }
        if (nextButton) {
            nextButton.addEventListener("click", () => shiftDay(1));
        }

        const isTypingTarget = (node) => {
            if (!node || !(node instanceof HTMLElement)) {
                return false;
            }
            return node.matches("input, textarea, select") || node.isContentEditable;
        };
        document.addEventListener("keydown", (event) => {
            if (event.ctrlKey || event.altKey || event.metaKey) {
                return;
            }
            if (isTypingTarget(document.activeElement)) {
                return;
            }
            if (event.key === "ArrowLeft") {
                event.preventDefault();
                shiftDay(-1);
                return;
            }
            if (event.key === "ArrowRight") {
                event.preventDefault();
                shiftDay(1);
                return;
            }
            if (event.key === "Home") {
                event.preventDefault();
                scrollToDayIndex(0);
                return;
            }
            if (event.key === "End") {
                event.preventDefault();
                scrollToDayIndex(chips.length - 1);
            }
        });

        let scrollRaf = 0;
        tableWrap.addEventListener("scroll", () => {
            if (scrollRaf) {
                return;
            }
            scrollRaf = window.requestAnimationFrame(() => {
                scrollRaf = 0;
                applyActive(resolveCurrentDayIndex());
            });
        });

        window.requestAnimationFrame(() => {
            scrollToDayIndex(defaultIndex, "auto");
        });
    }

    function initDragScroll(tableWrap) {
        let isDragging = false;
        let startX = 0;
        let startY = 0;
        let startLeft = 0;
        let startTop = 0;
        let moved = false;

        const resetDragging = () => {
            if (!isDragging) {
                return;
            }
            isDragging = false;
            tableWrap.classList.remove("is-dragging");
            if (moved) {
                tableWrap.dataset.dragMoved = "1";
                window.setTimeout(() => {
                    tableWrap.dataset.dragMoved = "";
                }, 0);
            }
        };

        tableWrap.addEventListener("mousedown", (event) => {
            if (event.button !== 0) {
                return;
            }
            if (event.target.closest("button, select, input, textarea, a, label")) {
                return;
            }
            isDragging = true;
            moved = false;
            startX = event.clientX;
            startY = event.clientY;
            startLeft = tableWrap.scrollLeft;
            startTop = tableWrap.scrollTop;
            tableWrap.classList.add("is-dragging");
            event.preventDefault();
        });

        window.addEventListener("mousemove", (event) => {
            if (!isDragging) {
                return;
            }
            const dx = event.clientX - startX;
            const dy = event.clientY - startY;
            if (Math.abs(dx) > 2 || Math.abs(dy) > 2) {
                moved = true;
            }
            tableWrap.scrollLeft = startLeft - dx;
            tableWrap.scrollTop = startTop - dy;
        });

        window.addEventListener("mouseup", resetDragging);
        tableWrap.addEventListener("mouseleave", resetDragging);
        tableWrap.addEventListener(
            "click",
            (event) => {
                if (tableWrap.dataset.dragMoved === "1") {
                    event.preventDefault();
                    event.stopPropagation();
                }
            },
            true
        );
    }

    function initTableControls() {
        initActionModals();
        initToolbarFilters();
        initSyncQuickRanges();
        initSyncProductPicker();
        initSheetsNavigation();

        const tableWrap = document.querySelector("[data-table-workspace]");
        if (!tableWrap) {
            return;
        }
        const updateUrl = tableWrap.dataset.noteUpdateUrl;
        if (updateUrl) {
            initInlineControls(tableWrap, updateUrl);
        }
        initLiveCalculations(tableWrap);
        initDensityToggle(tableWrap);
        initFullscreenToggle(tableWrap);
        initDayNavigation(tableWrap);
        initDragScroll(tableWrap);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initTableControls);
    } else {
        initTableControls();
    }
})();
