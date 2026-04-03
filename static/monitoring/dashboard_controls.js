(function () {
    const STORAGE_KEY = "wb-dashboard-active-op-tab";

    function resolveSafeTab(root, requestedTab) {
        const availableTabs = Array.from(root.querySelectorAll("[data-dashboard-tab-trigger]")).map(
            (node) => node.dataset.dashboardTabTrigger
        );
        const normalized = String(requestedTab || "").trim();
        if (normalized && availableTabs.includes(normalized)) {
            return normalized;
        }
        return availableTabs[0] || "sync";
    }

    function setActiveTab(root, requestedTab) {
        const activeTab = resolveSafeTab(root, requestedTab);
        root.dataset.activeTab = activeTab;

        root.querySelectorAll("[data-dashboard-tab-trigger]").forEach((trigger) => {
            const isActive = trigger.dataset.dashboardTabTrigger === activeTab;
            trigger.classList.toggle("is-active", isActive);
            trigger.setAttribute("aria-selected", isActive ? "true" : "false");
            trigger.setAttribute("tabindex", isActive ? "0" : "-1");
        });

        root.querySelectorAll("[data-dashboard-tab-panel]").forEach((panel) => {
            panel.hidden = panel.dataset.dashboardTabPanel !== activeTab;
        });

        try {
            sessionStorage.setItem(STORAGE_KEY, activeTab);
        } catch (_error) {
            // Ignore blocked storage.
        }
    }

    function initDashboardTabs(root) {
        const triggers = Array.from(root.querySelectorAll("[data-dashboard-tab-trigger]"));
        if (!triggers.length) {
            return;
        }

        triggers.forEach((trigger, index) => {
            trigger.addEventListener("click", () => {
                setActiveTab(root, trigger.dataset.dashboardTabTrigger);
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

        let preferredTab = root.dataset.activeTab || "sync";
        try {
            preferredTab = sessionStorage.getItem(STORAGE_KEY) || preferredTab;
        } catch (_error) {
            // Ignore blocked storage.
        }
        setActiveTab(root, preferredTab);
    }

    function bootstrap() {
        document.querySelectorAll("[data-dashboard-controls]").forEach((root) => {
            if (root.dataset.dashboardTabsBound === "1") {
                return;
            }
            root.dataset.dashboardTabsBound = "1";
            initDashboardTabs(root);
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bootstrap);
    } else {
        bootstrap();
    }
})();
