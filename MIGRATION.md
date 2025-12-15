# План миграции на Support Bot v2

## Текущее состояние

- **Старый бот:** `/bot/support-cfrd-bot.py`
  - Сервис: `cfrd-support-bot.service`
  - БД: `/bot/clients_main.db` (50 клиентов, 6831 сообщений, 8 менеджеров)
  - Сессия: `/bot/business_account.session`
  - Телефон: +79859482949
  - Группа: -1002675883945

- **Новый бот:** `/root/cfrd-support-bot-v2/bot.py`
  - БД: `clients.db` (после миграции)
  - Сессия: `business_account.session`
  - Группа: -1002675883945 (та же)

## Шаги миграции

### 1. Подготовка (без остановки бота)

```bash
# Перейти в директорию нового бота
cd /root/cfrd-support-bot-v2

# Запустить миграцию БД
./venv/bin/python migrate_db.py

# Активировать production конфиг
cp config.py config_test.py
cp config_production.py config.py
```

### 2. Копирование сессии

```bash
# Скопировать сессию из старого бота
cp /bot/business_account.session /root/cfrd-support-bot-v2/
```

### 3. Создание нового сервиса

```bash
# Создать systemd сервис (уже готов ниже)
sudo nano /etc/systemd/system/cfrd-support-bot-v2.service

# Перезагрузить systemd
sudo systemctl daemon-reload
```

### 4. Переключение (КРИТИЧНО - делать быстро!)

```bash
# Остановить старый бот
sudo systemctl stop cfrd-support-bot.service

# Запустить новый бот
sudo systemctl start cfrd-support-bot-v2.service

# Проверить статус
sudo systemctl status cfrd-support-bot-v2.service

# Проверить логи
tail -f /root/cfrd-support-bot-v2/bot.log
```

### 5. Проверка работы

1. Отправить тестовое сообщение от клиента
2. Проверить, что сообщение появилось в треде
3. Ответить из треда, проверить доставку
4. Проверить команды: /help, /team, /duties

### 6. Откат (если что-то пошло не так)

```bash
# Остановить новый бот
sudo systemctl stop cfrd-support-bot-v2.service

# Запустить старый бот
sudo systemctl start cfrd-support-bot.service
```

### 7. Финализация (после успешной проверки)

```bash
# Отключить старый сервис
sudo systemctl disable cfrd-support-bot.service

# Включить новый сервис в автозапуск
sudo systemctl enable cfrd-support-bot-v2.service

# Переименовать сервис (опционально)
# sudo mv /etc/systemd/system/cfrd-support-bot-v2.service /etc/systemd/system/cfrd-support-bot.service
```

---

## Файл сервиса

Создать `/etc/systemd/system/cfrd-support-bot-v2.service`:

```ini
[Unit]
Description=CFRD Support Bot v2
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/cfrd-support-bot-v2
ExecStart=/root/cfrd-support-bot-v2/venv/bin/python bot.py
Restart=always
RestartSec=10

# Логирование в файл bot.log (через редирект в самом боте)
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## Чеклист

- [ ] Бэкап старой БД сделан
- [ ] Миграция БД выполнена успешно
- [ ] config.py обновлён на production
- [ ] Сессия скопирована
- [ ] Systemd сервис создан
- [ ] Старый бот остановлен
- [ ] Новый бот запущен
- [ ] Тестовое сообщение доставлено
- [ ] Ответ из треда доставлен
- [ ] Команды работают
- [ ] Старый сервис отключен
- [ ] Новый сервис включен в автозапуск
