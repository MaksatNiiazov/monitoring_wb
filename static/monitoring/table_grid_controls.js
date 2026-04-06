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
        return `${formatDecimalValue(value)}%`;
    }

    function initLiveCalculations(tableWrap) {
        const table = tableWrap.querySelector("table");
        if (!table) {
            return;
        }

        const ROW = {
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
            OVERALL: 6,
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
            const clicks = readNumber(ROW.CLICKS, blockIndex, COL.OVERALL);
            const carts = readNumber(ROW.CARTS, blockIndex, COL.OVERALL);
            const orders = readNumber(ROW.ORDERS, blockIndex, COL.OVERALL);
            const orderSum = readNumber(ROW.ORDER_SUM, blockIndex, COL.OVERALL);
            const spend = readNumber(5, blockIndex, COL.OVERALL);

            const conversionCart = clicks ? (carts || 0) * 100 / clicks : 0;
            setCellText(ROW.CONVERSION_CART, blockIndex, COL.OVERALL, formatPercentValue(conversionCart));

            const conversionOrder = carts ? (orders || 0) * 100 / carts : 0;
            setCellText(ROW.CONVERSION_ORDER, blockIndex, COL.OVERALL, formatPercentValue(conversionOrder));

            const buyoutPercent = readNumber(ROW.BUYOUT_PERCENT, blockIndex, COL.INPUT_MAIN) || 0;
            const buyoutFraction = Math.abs(buyoutPercent) > 1 ? buyoutPercent / 100 : buyoutPercent;
            const buyouts = orderSum && buyoutFraction ? orderSum * buyoutFraction : 0;
            setCellText(ROW.BUYOUTS, blockIndex, COL.OVERALL, formatDecimalValue(buyouts));

            const drrSalesRatio = buyouts ? (spend || 0) / buyouts : 0;
            setCellText(ROW.DRR_SALES, blockIndex, COL.OVERALL, formatPercentValue(drrSalesRatio * 100));

            const sellerPrice = readNumber(ROW.SELLER_PRICE, blockIndex, COL.SELLER_PRICE) || 0;
            const unitCost = readNumber(ROW.UNIT_COST, blockIndex, COL.INPUT_MAIN) || 0;
            const logistics = readNumber(ROW.LOGISTICS, blockIndex, COL.INPUT_MAIN) || 0;
            const totalOrders = orders || 0;

            let profit = 0;
            if (sellerPrice && buyoutFraction > 0) {
                const logisticsAdjustment = logistics / buyoutFraction - 50;
                const margin =
                    sellerPrice -
                    unitCost -
                    (sellerPrice * drrSalesRatio) / 100 -
                    sellerPrice * 0.25 -
                    logisticsAdjustment;
                profit = margin * totalOrders * buyoutFraction;
            }
            setCellText(ROW.PROFIT, blockIndex, COL.INPUT_MAIN, formatDecimalValue(profit));
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

        const hydrateStocksModal = (trigger) => {
            const stocksModalHead = stocksModal ? stocksModal.querySelector("[data-stocks-modal-head]") : null;
            if (!stocksModal || !stocksModalMeta || !stocksModalBody || !stocksModalHead || !trigger) {
                return;
            }
            const payload = parseStockPayload(trigger.getAttribute("data-stock-payload") || "");
            const dateLabel = String(trigger.getAttribute("data-stock-date") || "").trim();
            const totalValue = Number.parseInt(String(trigger.getAttribute("data-stock-total") || "0"), 10);
            const totalLabel = Number.isFinite(totalValue) ? totalValue.toLocaleString("ru-RU") : "0";
            stocksModalMeta.textContent = dateLabel
                ? `Срез на ${dateLabel}. Итого по складам: ${totalLabel} шт.`
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
                if (openTrigger.matches("[data-stock-popup-button]")) {
                    hydrateStocksModal(openTrigger);
                }
                const targetId = openTrigger.dataset.modalOpen;
                const modal = targetId ? document.getElementById(targetId) : null;
                if (!modal || !modal.matches("[data-table-modal]")) {
                    return;
                }
                event.preventDefault();
                openModal(modal, openTrigger);
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
        const presetsRoot = document.querySelector("[data-table-filter-presets]");
        const presetButtons = presetsRoot ? Array.from(presetsRoot.querySelectorAll(".table-preset-button")) : [];

        const clampHistoryDays = () => {
            if (!historyInput) {
                return;
            }
            const fallback = 14;
            const parsed = Number(historyInput.value || fallback);
            const normalized = Number.isFinite(parsed) ? Math.max(1, Math.min(90, Math.round(parsed))) : fallback;
            historyInput.value = String(normalized);
        };

        const updateActivePreset = () => {
            if (!historyInput || !presetButtons.length) {
                return;
            }
            const currentDays = String(historyInput.value || "").trim();
            presetButtons.forEach((button) => {
                if (!button.dataset.filterDays) {
                    return;
                }
                button.classList.toggle("is-active", button.dataset.filterDays === currentDays);
            });
        };

        if (historyInput) {
            historyInput.addEventListener("change", () => {
                clampHistoryDays();
                updateActivePreset();
            });
            historyInput.addEventListener("blur", () => {
                clampHistoryDays();
                updateActivePreset();
            });
            clampHistoryDays();
            updateActivePreset();
        }

        if (!presetButtons.length) {
            return;
        }

        const todayValue = () => {
            const now = new Date();
            const year = now.getFullYear();
            const month = String(now.getMonth() + 1).padStart(2, "0");
            const day = String(now.getDate()).padStart(2, "0");
            return `${year}-${month}-${day}`;
        };

        presetButtons.forEach((button) => {
            button.addEventListener("click", () => {
                let changed = false;

                if (button.dataset.filterDays && historyInput) {
                    historyInput.value = button.dataset.filterDays;
                    changed = true;
                }
                if (button.dataset.filterReference === "today" && referenceInput) {
                    referenceInput.value = todayValue();
                    changed = true;
                }

                clampHistoryDays();
                updateActivePreset();

                if (changed) {
                    form.requestSubmit();
                }
            });
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
