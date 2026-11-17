# Статус приёмки17.11.2026

Production: NO-GO. Полный live CSI sanity дал50PASS/2FAIL/1pending;
неподдерживаемые optional features отдельно skipped. Ошибка расширения свежего
тома воспроизведена напрямую native API CLO без CSI. Фиксированная задержка
перед resize не принята как гарантия готовности тома.

Отдельно проверены RWO/ext4 create/mount/data hashes, reboot и8дисков, штатный
перенос PVC, явное выключение потерянной VM, recovery/restore и зрелый RW resize.
Последующие safety fixes подтверждены unit/race и15cases на реальном local
Kubernetes API; повторного полного native sanity финального образа ещё не было.

Публичная упаковка и отдельный Go-модуль не закрывают эти ограничения.

Новый публичный namespace дополнительно проверен на отдельном Kubernetes1.36.6:
15/15 initialization/recovery cases с настоящими CRD/RBAC,71,087с;
тестовый локальный кластер удалён. Это не заменяет full native CLO sanity.
