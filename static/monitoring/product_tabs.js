(function () {
    const STORAGE_KEY = "wb-product-detail-active-tab";

    function getVisiblePanelCount(stack) {
        return Array.from(stack.querySelectorAll("[data-detail-tab-panel]")).filter((panel) => !panel.hidden).length;
    }

    function syncLayout(shell) {
        const stacks = Array.from(shell.querySelectorAll("[data-detail-stack]"));
        let visibleStacks = 0;
        stacks.forEach((stack) => {
            const hasVisiblePanels = getVisiblePanelCount(stack) > 0;
            stack.hidden = !hasVisiblePanels;
            if (hasVisiblePanels) {
                visibleStacks += 1;
            }
        });
        shell.classList.toggle("is-single-column", visibleStacks <= 1);
    }

    function resolveSafeTab(shell, requestedTab) {
        const panels = Array.from(shell.querySelectorAll("[data-detail-tab-panel]"));
        const normalized = String(requestedTab || "").trim();
        if (normalized && panels.some((panel) => panel.dataset.detailTabPanel === normalized)) {
            return normalized;
        }
        return shell.querySelector("[data-detail-tab-trigger]")?.dataset.detailTabTrigger || "overview";
    }

    function setActiveTab(shell, nextTab) {
        const activeTab = resolveSafeTab(shell, nextTab);
        shell.dataset.activeTab = activeTab;

        shell.querySelectorAll("[data-detail-tab-trigger]").forEach((trigger) => {
            const isActive = trigger.dataset.detailTabTrigger === activeTab;
            trigger.classList.toggle("is-active", isActive);
            trigger.setAttribute("aria-selected", isActive ? "true" : "false");
            trigger.setAttribute("tabindex", isActive ? "0" : "-1");
        });

        shell.querySelectorAll("[data-detail-tab-panel]").forEach((panel) => {
            panel.hidden = panel.dataset.detailTabPanel !== activeTab;
        });

        syncLayout(shell);

        try {
            sessionStorage.setItem(STORAGE_KEY, activeTab);
        } catch (error) {
            // Session storage can be blocked by browser policy.
        }
    }

    function initDetailTabs(shell) {
        const triggers = Array.from(shell.querySelectorAll("[data-detail-tab-trigger]"));
        if (!triggers.length) {
            return;
        }

        triggers.forEach((trigger, index) => {
            trigger.addEventListener("click", () => {
                setActiveTab(shell, trigger.dataset.detailTabTrigger);
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

        let preferredTab = shell.dataset.activeTab;
        try {
            preferredTab = sessionStorage.getItem(STORAGE_KEY) || preferredTab;
        } catch (error) {
            // Ignore unavailable session storage.
        }
        setActiveTab(shell, preferredTab);
    }

    function bootstrap() {
        document.querySelectorAll("[data-detail-tabs-shell]").forEach((shell) => {
            if (shell.dataset.tabsBound === "1") {
                return;
            }
            shell.dataset.tabsBound = "1";
            initDetailTabs(shell);
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bootstrap);
    } else {
        bootstrap();
    }
})();
