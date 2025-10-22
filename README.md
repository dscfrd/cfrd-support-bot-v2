# CFRD Support Bot v2

Telegram бот для управления поддержкой клиентов с использованием форумов Telegram.

## Особенности

- 🔄 Автоматическое создание тредов для каждого клиента
- 👥 Система назначения ответственных менеджеров
- 🔔 Уведомления о неотвеченных сообщениях
- 📁 Система хранения файлов
- 📊 Статистика по клиентам и менеджерам
- 🏷️ Уникальные ID для клиентов

## Архитектура

Проект имеет модульную структуру:

```
cfrd-support-bot-v2/
├── main.py                 # Точка входа
├── config.py               # Конфигурация
├── bot/
│   ├── database/           # Слой работы с БД
│   │   ├── connection.py
│   │   └── queries.py
│   ├── handlers/           # Обработчики событий
│   │   ├── client_messages.py
│   │   └── manager_commands.py
│   ├── services/           # Бизнес-логика
│   │   ├── thread_service.py
│   │   ├── notification_service.py
│   │   ├── manager_service.py
│   │   ├── media_service.py
│   │   └── storage_service.py
│   └── utils/              # Вспомогательные функции
│       └── helpers.py
└── requirements.txt
```

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd cfrd-support-bot-v2
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Создайте файл `.env` на основе `.env.example`:
```bash
cp .env.example .env
```

4. Заполните `.env` вашими данными:
```
API_ID=your_api_id
API_HASH=your_api_hash
PHONE_NUMBER=your_phone_number
SUPPORT_GROUP_ID=your_support_group_id
STORAGE_CHANNEL_ID=your_storage_channel_id
```

## Запуск

### Вариант 1: Обычный запуск
```bash
python main.py
```

### Вариант 2: Запуск через systemd (рекомендуется для production)

1. Отредактируйте файл `cfrd-support-bot.service`:
```bash
nano cfrd-support-bot.service
```

Замените:
- `YOUR_USER` на имя пользователя системы (например, `ubuntu`)
- `/path/to/cfrd-support-bot-v2` на полный путь к проекту (например, `/home/ubuntu/cfrd-support-bot-v2`)

2. Создайте директорию для логов:
```bash
sudo mkdir -p /var/log/cfrd-support-bot
sudo chown YOUR_USER:YOUR_USER /var/log/cfrd-support-bot
```

3. Установите service:
```bash
sudo cp cfrd-support-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cfrd-support-bot
```

4. Управление ботом:
```bash
# Запустить бота
sudo systemctl start cfrd-support-bot

# Остановить бота
sudo systemctl stop cfrd-support-bot

# Перезапустить бота
sudo systemctl restart cfrd-support-bot

# Проверить статус
sudo systemctl status cfrd-support-bot

# Посмотреть логи
sudo journalctl -u cfrd-support-bot -f

# Или логи из файлов
tail -f /var/log/cfrd-support-bot/bot.log
tail -f /var/log/cfrd-support-bot/bot-error.log
```

## Команды для менеджеров

### Основные команды:
- `/auth [эмодзи], [Имя], [Должность], [4 цифры]` - Авторизация менеджера
- `/{ID_треда} [текст]` - Ответить клиенту по ID треда
- `/#{ID_клиента} [текст]` - Ответить клиенту по его уникальному ID
- `/wtt` - Получить информацию о текущем треде
- `/threads` - Показать список всех активных тредов
- `/myinfo` - Просмотреть свою информацию
- `/help` - Показать справку

### Управление:
- `/set_id {ID_треда} [желаемый_ID]` - Назначить ID клиенту
- `/card {ID_треда}` - Отправить свою карточку клиенту

## Особенности работы

### Автоназначение ответственного
Первый менеджер, ответивший на обращение клиента, автоматически назначается ответственным за этого клиента.

### Система уведомлений
- Если клиент не получил ответ в течение 10 минут, тред помечается как срочный
- Система отправляет уведомления ответственному менеджеру о неотвеченных сообщениях
- Интервалы уведомлений настраиваются в `config.py`

### Хранилище файлов
Менеджеры могут загружать файлы в хранилище и отправлять их клиентам по команде.

## База данных

Бот использует SQLite для хранения:
- Информации о клиентах
- Истории сообщений
- Данных менеджеров
- Статусов тредов
- Файлов в хранилище

База данных создается автоматически при первом запуске.

## Логирование

Логи записываются в файл `bot.log` и выводятся в консоль.

## Разработка

### Структура модулей

- **database/** - Работа с базой данных
- **handlers/** - Обработчики сообщений и команд
- **services/** - Бизнес-логика (управление тредами, уведомления, и т.д.)
- **utils/** - Вспомогательные функции

### Добавление новых функций

1. Создайте сервис в `bot/services/`
2. Создайте обработчик в `bot/handlers/`
3. Зарегистрируйте обработчик в `main.py`

## Лицензия

MIT

## Поддержка

По вопросам обращайтесь к разработчикам проекта.
