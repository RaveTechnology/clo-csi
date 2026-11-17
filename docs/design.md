# CSI v1: основные гарантии

Один controller управляет новыми RWO/ext4 Filesystem томами. Node agent проверяет
cloud/guest identity и изучает serial только по единственной доказанной attach
delta; путь устройства из API не считается идентификатором. Serial и ext4 UUID
сохраняются в долговечном Kubernetes registry до первого format.

UID/resourceVersion-bound intents и immutable initialization seal защищают
создание записей, claim version защищает node/volume identity между admission
и исполнением. Истёкшая lease не является ограждением старого процесса: node
agent проверяет PID/startTicks/bootID/PodUID через hostPID.

При потере ноды оператор явно выключает точную старую VM в CLO и подтверждает
fence через admin-команды. Сам CSI VM не выключает. Restore сохраняет epoch,
не доверяет устаревшей резервной копии как доказательству остановки controller
и требует отдельного ограждения при утрате live epoch. Подробные процедуры в README.

RWX, raw block, snapshots/clones и импорт существующих дисков не реализованы.
Production gate и полная проверка нового финального образа остаются открытыми.
