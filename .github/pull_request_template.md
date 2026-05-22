## Summary

- 

## Verification

- [ ] `python3 -m pytest -q`
- [ ] `python3 migrate.py --db-dir ./data --verify`
- [ ] Focused checks:

## Authority And Safety

- [ ] Kernel authority, gates, budgets, side effects, and route mutation remain fail-closed.
- [ ] No live Hermes, paid-provider, customer-delivery, or dashboard-write authority was enabled.
- [ ] `../CURRENT_STATE.md` was updated only if live status or next priority changed.
