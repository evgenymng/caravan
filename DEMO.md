# Сценарий демонстрации

Два ноутбука в одной сети: A (отправитель) и B (получатель, IP `192.168.1.15`).
Для показа на одной машине замените `192.168.1.15` на `127.0.0.1`, а каталоги разнесите.

## Подготовка

```bash
make build
dd if=/dev/urandom of=big.bin bs=1048576 count=100   # 100 chunks по 1 MB; на macOS и Linux
shasum -a 256 big.bin
```

## 1. Обычная передача

B:

```bash
./bin/caravan receive --addr :8080 --dir ./downloads --log-level debug
```

A:

```bash
./bin/caravan send --file big.bin --addr 192.168.1.15:8080 --chunk-size 1MB --log-level debug
```

Что показать:

- в логе A: `hashing file`, `transfer prepared` с transfer ID, числом chunks и SHA-256;
- в логе B: `transfer opened`, на `debug` — `chunk stored chunk=N sha256=…` на каждую часть;
- в логе A: `chunk sent` / `chunk acked` — подтверждение частей;
- строку прогресса (байты, проценты, chunks, скорость, ETA);
- итог A: `completed: transfer …, sha256 …`, `chunks sent 100, acked 100, skipped on resume 0`;
- `shasum -a 256 downloads/big.bin` на B совпадает с A;
- `./bin/caravan status --dir ./downloads` показывает `status: completed`; `.part` и bitmap удалены.

## 2. Разрыв и продолжение (главный сценарий)

Удалите `downloads/big.bin` на B, чтобы не было коллизии имени. Запустите B как в п. 1.

A — вариант со скрытым флагом (разрыв сразу после отправки chunk 49, до получения его ACK):

```bash
./bin/caravan send --file big.bin --addr 192.168.1.15:8080 --debug-drop-after-chunk 49
```

A — вариант с реальным разрывом: запустите `send` без флага и на ~50 % выключите Wi-Fi
на A (или выдерните кабель), подождите несколько секунд, включите обратно.

Что показать:

1. A: `dropping connection on purpose after_chunk=49` (или `transfer interrupted`),
   затем `reconnecting attempt=1 delay=1s`.
2. B: `session ended … EOF`; `cat downloads/.caravan/*/metadata.json` — статус
   `interrupted`; `./bin/caravan status --dir ./downloads` показывает `50 / 100 chunks`.
3. A после переподключения: `resume state received=50 ranges=[[0 49]]` — это ответ
   получателя `RESUME_STATE`; отправитель узнаёт состояние именно отсюда.
4. A продолжает с chunk 50 (`chunk sent chunk=50`), chunk 49 **не** пересылается, хотя ACK
   на него A не получал.
5. Итог A: `chunks sent 100, acked 99, skipped on resume 50, acked-and-resent 0, reconnects 1`.
6. SHA-256 на обеих сторонах совпадает.

## 3. Перезапуск получателя

Запустите передачу (можно с `--retry-delay 2s --max-retries 0`), на середине убейте B:

```bash
kill -9 $(pgrep -f 'caravan receive')
./bin/caravan status --dir ./downloads     # transferring → interrupted, N / 100 chunks
./bin/caravan receive --addr :8080 --dir ./downloads
```

B при старте пишет `stored transfer … status=interrupted received=N`. A переподключается,
получает `RESUME_STATE` с диска B и досылает только недостающее.

## 4. Повреждение chunk

Воспроизводится тестом `TestCorruptedChunkIsRejectedAndResent` (`go test -run Corrupted
./tests/ -v`): proxy портит байт в данных chunk, получатель отвечает `ERROR bad_checksum`,
бит в bitmap не ставится, отправитель повторяет chunk и передача завершается успешно.

## 5. Таймауты

Отправитель ждёт ответ не дольше `--ack-timeout`:

```bash
# B: вместо получателя — сокет, который принимает соединение и молчит
python3 -c 'import socket,time; s=socket.socket(); s.bind(("",8080)); s.listen(); c,_=s.accept(); time.sleep(600)'
# A:
./bin/caravan send --file big.bin --addr 192.168.1.15:8080 --ack-timeout 5s --max-retries 2
```

A: `transfer interrupted error="read tcp …: i/o timeout"` (HELLO_OK не пришёл за 5 с),
переподключения, затем `giving up after 2 retries`.

Получатель закрывает молчащее соединение по `--read-timeout`:

```bash
# B:
./bin/caravan receive --addr :8080 --dir ./downloads --read-timeout 5s
# A:
nc 192.168.1.15 8080      # ничего не вводить
```

Через 5 с B пишет `session ended … i/o timeout`, `nc` завершается.

Таймаут ACK на живом получателе: `receive --debug-ack-delay 40s` при `send --ack-timeout 30s`.

## 6. Корректное завершение

Во время передачи нажмите `Ctrl+C` на B: лог `shutting down`, сессия сохраняет статус
`interrupted`, процесс выходит; `status` показывает принятые chunks. То же на A:
`interrupted`, повторный `send` продолжает передачу.

## 7. Тесты

```bash
go test ./...          # все сценарии, включая 100 MB
go test -race ./...
go test -short ./...   # без 100 MB
```

## Вопросы на защите

**Откуда sender после reconnect знает, что уже передано?**
Из ответа `RESUME_STATE` получателя: после `META` получатель читает bitmap с диска и
отправляет список диапазонов полученных chunks (`[[0,49]]`). Отправитель строит дополнение
и шлёт только его. Отправитель своей памяти о прошлой сессии не доверяет: ACK мог не дойти.

**Почему потерянный ACK не обязательно означает потерянный chunk?**
ACK отправляется после того, как chunk проверен, записан и сброшен на диск. Если
соединение умерло между записью и доставкой ACK, chunk у получателя есть. Поэтому после
переподключения отправитель сначала спрашивает состояние, а не пересылает «неподтверждённое».

**Почему совпадение checksum каждого chunk не отменяет итоговую проверку файла?**
Per-chunk SHA-256 защищает содержимое одной части, но не гарантирует, что все части
записаны по правильным смещениям, что не осталось дыр, что `.part` не повреждён между
сессиями и что исходный файл не изменился между попытками. Итоговый SHA-256 проверяет
результат как единое целое.

**Почему TCP сам не решает задачу продолжения после нового соединения?**
TCP гарантирует порядок и доставку байтов внутри одного живого соединения. Новое
соединение — новый поток с нуля: получатель не знает, что уже принимал раньше, а
отправитель — что из отправленного дошло. Сопоставить старое и новое соединение может
только прикладной протокол: transfer ID + сохранённое состояние + `RESUME_STATE`.
