# CLO CSI

**Предварительный выпуск0.1.0-rc.2; production gate закрыт.** Свежий native volume
resize в CLO воспроизводимо переходил в ERROR, включая прямой API без CSI.
Полный live sanity:50PASS/2FAIL/1pending; это не production-ready CSI.


`clo.csi.rave-technology.io` создаёт **новые RWO/ext4 Filesystem** PVC. Несколько pod
на одной ноде и read-only bind поддерживаются. RWX/RWOP, raw block, импорт,
snapshot и clone не реализованы. Production gate закрыт: полный live sanity
дал50 PASS/2 FAIL из-за ERROR при увеличении свежего свободного диска в CLO;
ошибка воспроизведена прямым API без CSI. Нужны подтверждённая готовность диска
или исправление CLO и новый успешный полный прогон. Recovery/backup, реальный
RW resize зрелого PVC и сохранность данных проверены отдельно. Результаты:
[CSI acceptance](docs/acceptance.md).
Дизайн и ограничения:
[CSI design](docs/design.md).

Сборка: `make csi-package` (binary и Helm archive), `make csi-image` (только
локальный Docker image). Артефакты имеют детерминированные tar metadata/gzip;
сверяйте `dist/SHA256SUMS`. Официальные sidecars, builder и runtime
закреплены linux/amd64 digest в `deploy/csi/images.lock.yaml`. APT использует
[Ubuntu snapshot](https://snapshot.ubuntu.com/) 20261113T000000Z. Сборка
требует доступа к этим registry и snapshot; загрузка образа в registry —
отдельная операция. Chart принимает собственный immutable image digest.

Предпосылки для узлов Ubuntu24.04/RKE2 1.36: util-linux, e2fsprogs, работающий
udev/SCSI rescan, стандартный kubelet root `/var/lib/kubelet`, identity0600
`/etc/clo/machine-identity.json` из актуального Node Driver. Identity содержит
сохранённые project/name/attempt, без токена. CLO API device path не является
идентификатором: serial изучается по единственной проверенной cloud+guest
delta и сохраняется до первого format. Лимит 8 data volumes требует живой
проверки attach/reboot; тесты с fake host не подтверждают kernel behavior.

CSI-enabled ноды получают `clo.csi.rave-technology.io/enabled=true`; выбранный
control-plane host дополнительно `clo.csi.rave-technology.io/controller=true`.
DaemonSet допускает control-plane taints. На этом хосте node agent должен
проверить фактический PID/start ticks/boot ID/PodUID/nonce controller через
hostPID `/proc`, до первого cloud POST. Одной истёкшей lease недостаточно.
Нужны описанные выше CLO policy routing и private Canal; сеть должна пропускать
pod→private HTTPS Service и node→Kubernetes API для TokenReview.

Chart не создаёт cloud credentials. Подготовьте отдельные `existingSecret`
с ключом `token` и TLS Secret; токен монтируется только controller driver.
Node получает только CA и короткий projected SA token с audience
`clo-csi-node`. Никаких cloud токенов в values/cloud-init/CRD. Service
`clo-csi-controller` — private ClusterIP, bootstrap доступен до Ready;
`/livez` не зависит от mutating readiness. Установка требует заранее
сохранённый installation UUID; immutable ConfigMap и registry надо резервировать.
Контроллер использует общий бюджет CLO HTTP API — один запрос в секунду,
включая pagination. При429 учитывается Retry-After, повторяется только GET
(до трёх повторов); POST/DELETE не повторяются транспортом. Успешная полная
регистрация node повторяется раз в5минут, неуспешная — через10секунд; проверки
ownership/attachment/fence перед операциями остаются свежими. Это консервативный
профиль после наблюдавшегося ограничения в lab, а не гарантия числовой квоты CLO.
Sidecar'ы выполняют по одной native операции одновременно. На каждой ноде
`/register`, `/stage` и `/report` ждут своей очереди **до** запуска HTTP deadline;
проверки владения выполняются заново. Binding, job polling и receipts используют
независимый канал, чтобы ожидание cloud proof не блокировало recovery.
Пример команд использует явно выбранные scope и существующие секреты:

```sh
CSI_CONTEXT=YOUR_LAB_CONTEXT
CSI_PROJECT=YOUR_PROJECT_UUID
CSI_INSTALLATION=YOUR_PERSISTED_INSTALLATION_UUID
bin/clo-csi admin tls-bootstrap --context="$CSI_CONTEXT" --project-id="$CSI_PROJECT" \
  --installation-id="$CSI_INSTALLATION" --out=/PRIVATE/DIR/clo-csi-tls.yaml
kubectl --context="$CSI_CONTEXT" create namespace clo-system
kubectl --context="$CSI_CONTEXT" apply -f /PRIVATE/DIR/clo-csi-tls.yaml
# Helm upgrade does not upgrade CRDs; update schemas before the controller.
kubectl --context="$CSI_CONTEXT" apply -f deploy/csi/crds
helm upgrade --install clo-csi deploy/csi --kube-context="$CSI_CONTEXT" --namespace=clo-system \
  --set-string projectID="$CSI_PROJECT" --set-string installationID="$CSI_INSTALLATION" \
  --set-string existingSecret=YOUR_EXISTING_CLO_SECRET \
  --set-string image.repository=YOUR_IMAGE --set-string image.digest=YOUR_SHA256_DIGEST
bash scripts/check-csi-lab.sh "$CSI_CONTEXT" "$CSI_PROJECT" "$CSI_INSTALLATION"
```

StorageClass `clo-volumes` не default: Retain, WaitForFirstConsumer, expansion.
Удаление PVC сохраняет диск. Explicit Delete StorageClass допустим только для
приёмочных данных либо после отдельного решения об уничтожении данных.
Helm uninstall/удаление CRD не является процедурой очистки storage: сначала
сверить полный registry, PV, cloud inventory и резервные копии. Не снимайте
finalizer и не меняйте immutable spec, чтобы обойти блокировку.

## Инициализация Kubernetes registry

До обновления controller примените актуальные CRD schemas отдельно: Helm upgrade
сам их не обновляет. Новые записи сохраняют immutable creation intent в spec,
пишут начальный status и затем ставят UID/resourceVersion-bound metadata seal.
Неоконченная запись недоступна обычным операциям; следующий admission/restart
заканчивает только исходный, ещё не использованный intent. Это дополнительные
Kubernetes List/CAS, без CLO POST. Полные root/boot facts новой ноды входят в её
первое сохранение. Два ответа об одном создании сохраняют один UID.

Потерянный status уже sealed записи или старой записи без intent не сбрасывается:
требуются freeze, backup и разбор оператором. Не снимайте initialization marker
и не редактируйте status для обхода запрета. Не откатывайте старый controller,
который не понимает seal, при pending initialization. Сохраняются один controller,
Recreate и обязательное доказательство прекращения старого issuer. Протокол и
проверки: [описание гарантий](docs/design.md).

## CSI recovery runbook

Все admin команды обращаются к **тому же запущенному coordinator** через
scoped kubectl exec и закрытый Unix socket. Для каждой команды передавайте
`--context`, `--project-id`, `--installation-id`; namespace default clo-system.
Изменения содержат `--operator` и UUID `--nonce` (по умолчанию локальный USER
и новый UUID). Сначала inspect/dry-run; IDs — логические имена записей из
ответа inspect, а не metadata UID или device path. Для повторения одного
request сохраняйте тот же nonce и входные параметры.

| Ситуация | Действие |
|---|---|
| Неясный результат POST | `admin inspect --operation o-…`, затем `admin reconcile --operation o-… --dry-run`. При доказанном однозначном результате повторить без dry-run. Dispatched POST не переотправляется. Alive/Unknown issuer или неразрешённый native job сохраняют lock. |
| Потеря ноды | Оператор **явно выключает точную VM в CLO**, ждёт STOPPED/OFF; `admin record-fence --binding n-… --dry-run`, затем без dry-run. Сохранить возвращённый `f-…`. CSI не выполняет power operations. Старую VM не включать до полного detach её PVC. |
| Прерванный attach/enrollment | После verified fence: `admin recover-attachment --operation o-… --fence f-… --dry-run`, затем без dry-run. Никакого ручного выбора serial; новый attach заново проверяет baseline. Pending native job не разблокируется одним фактом выключения процесса. |
| Quarantine | Устранить неизвестный диск/FS/mount/операцию, затем `admin clear-quarantine --binding n-… --dry-run`. Изменение разрешено только после полной свежей cloud+guest проверки. |
| Неоднозначное создание неиспользованного тома | `admin retire-unpublished --volume v-… --dry-run`. Только never-published/unformatted; tombstone/имя сохраняются для позднего orphan. Уже использованные данные так не списываются. |
| Прерванный первый format | Reconcile принимает только собственный ext4 UUID+serial после read-only unmounted e2fsck; частичный/чужой FS остаётся blocked. Второй автоматический mkfs запрещён. |

Обычный backup: `admin freeze` дожидается admitted writers, затем `admin export
--out=/PRIVATE/DIR/registry.json`; файл0600 хранить **вне зависимого кластера**.
Затем `admin resume`. `admin restore --file=/PRIVATE/DIR/registry.json` требует
freeze/drain, верные scope/checksum/graph и сверку реального cloud inventory.
Существующий более новый controller epoch сохраняется. Нельзя подменять
installation UUID, угадывать serial или создавать пустой registry поверх
собственных существующих дисков.

Если потерян сам live epoch, историческая копия не доказывает отсутствие
более нового paused controller. Restore остаётся Frozen/RestoreNeedsFence.
Консервативное восстановление v1 требует всех **остальных VM проекта** в
STOPPED/OFF и проверенный maintenance host, где полный `/proc` scan видит
только текущий controller этой installation. Это может требовать временной
остановки management и отдельного boot-only maintenance API. Оператор
организует отключение; CSI ничего не выключает. Даже после этого незавершённые
native jobs требуют отдельной сверки, их POST не повторяется.

TLS bootstrap выдаёт сертификат на 90 дней, планируйте ротацию заранее.
`admin tls-bootstrap --previous-tls=/PRIVATE/old.yaml --out=/PRIVATE/overlap.yaml`
с теми же scope создаёт два0600 файла. Сначала применить overlap (старый cert/key,
обе CA), перезапустить node DaemonSet и дождаться регистрации всех нод. Затем
применить `.activate.yaml` (новый cert/key, обе CA); controller перечитывает
serving key pair. Удалять старую CA можно после проверки всех клиентов.
Не выводите эти manifest в терминал, Git или логи.

Живые результаты, ограничения и состояние приёмки:
[CLO CSI acceptance](docs/acceptance.md).

## Публичные артефакты

[Releases](https://github.com/RaveTechnology/clo-csi/releases) содержат binary,
Helm `.tgz`, image digest и SHA256SUMS. Образ: `ghcr.io/ravetechnology/clo-csi`.
При установке задавайте digest конкретного выпуска вместо mutable tag.
Chart и binary публикуются с одинаковой версией; driver identity
`clo.csi.rave-technology.io` согласована с CRD и node labels публичного выпуска.
Установка с другим namespace требует отдельной миграции: [граница identity](docs/identity.md). Общая библиотека — clo-cloud-go v0.1.0.

Обычные `make check` и `make package` не создают облачных ресурсов. Live sanity
явно opt-in и требует отдельного disposable lab; в GitHub Actions он не включён.
Лицензия собственного кода Apache-2.0; зависимости и base image имеют свои лицензии.
