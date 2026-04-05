Project: WB Monitoring

Purpose
- Internal dashboard for WB analytics, monitoring table, sync, and reports.
- Primary UX is the table workspace. Keep it fast and predictable.

Key Areas
- Table rendering: monitoring/services/monitoring_table.py
- Export: monitoring/services/exporters.py
- Reports: monitoring/services/reports.py
- Inline table edits: monitoring/views.py + static/monitoring/table_grid_controls.js
- Sync: monitoring/services/sync.py

Conventions
- Prefer UTF-8 files. Keep Russian labels as normal Unicode strings.
- Avoid hard-coded row numbers when layout can change; compute offsets.
- When changing table rows, update:
  - monitoring_table.py (rows + formulas)
  - exporters.py (export rows)
  - views.py (editable controls)
  - table_grid_controls.js (live calculations)

Quality Checks
- Run: python -m py_compile monitoring/services/monitoring_table.py monitoring/services/exporters.py monitoring/views.py
- Smoke check table UI after label changes.

Notes
- Keep UI text consistent across table, reports, and export.
- If strings show as “????”, it means the source has literal question marks.
