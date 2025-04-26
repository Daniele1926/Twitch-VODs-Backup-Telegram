# Twitch-VODs-Backup-Telegram

An asynchronous Python script to automatically download Twitch VODs (Video On Demand) and continuously upload them to Telegram channels.

---

## 📦 Features

- **Check and download new VODs** from a specified Twitch channel
- **Robust download management** with exponential backoff retries and token refresh (user/app)
- **Video splitting** into configurable size segments with final merge and optimization
- **Metadata correction** (faststart, moov atom) for optimal streaming
- **Telegram upload** via Telethon with auto-generated thumbnails
- **SQLite database** to track VOD statuses (`pending`, `processing`, `live`, `completed`, `failed`)
- **Centralized logging** with `tqdm` progress bar integration
- **Hot-reloadable configuration** without restarting the script

---

## 🛠️ Requirements

- Python 3.8+
- ffmpeg and ffprobe installed and available in PATH
- `pip install -r requirements.txt`

**Core dependencies**:

```text
aiohttp
aiosqlite
aiofiles
yt-dlp
telethon
tqdm
psutil
FastTelethonhelper
```

---

## ⚙️ Configuration

```json
{
  "TWITCH_CLIENT_ID": "<YOUR_TWITCH_CLIENT_ID>",
  "TWITCH_CLIENT_SECRET": "<YOUR_TWITCH_CLIENT_SECRET>",
  "TWITCH_CHANNEL_ID": "<YOUR_TWITCH_CHANNEL_ID>",
  "USER_AUTH_TOKEN": "<OPTIONAL_USER_OAUTH_TOKEN>",
  "USER_REFRESH_TOKEN": "<OPTIONAL_REFRESH_TOKEN>",
  "TELEGRAM_API_ID": "<YOUR_TELEGRAM_API_ID>",
  "TELEGRAM_API_HASH": "<YOUR_TELEGRAM_API_HASH>",
  "TELEGRAM_PHONE_NUMBER": "+<YOUR_PHONE_NUMBER>",
  "DATABASE_NAME": "vods.db",

  "TELEGRAM_CHANNELS": [
    { "id": "<CHANNEL_ID>", "name": "NomeCanale" },
    { "id": "<CHANNEL_ID_BACKUP>", "name": "NomeCanaleBackup" }
  ],

  "VOD_SETTINGS": {
    "MIN_RETRY_DELAY": 300,
    "MAX_RETRY_DELAY": 86400,
    "AUTO_CHECK_VODS": true,
    "PHRASE_IN_THUMBNAIL_URL": ["404_processing"],
    "VIDEO_QUALITY_VOD": ["best", "1080p60", "720p30"]
  },

  "SPLIT_SETTINGS": {
    "MAX_FILE_SIZE_MB": 2024,
    "MIN_FILE_SIZE_MB": 1800,
    "MAX_RETRIES_SPLIT": 10,
    "MERGE_THRESHOLD_MB": 100,
    "MID_TARGET_RATIO": 0.50,
    "UPLOAD_DELAY": 3,
    "PROCESSING_INTERVAL": 1800,
    "DROP_SEGMENT_THRESHOLD_SEC": 30.0
  },

  "VOD_ORDERING": {
    "field": "created_at",
    "order": "asc"
  }
}
```

### Configuration Notes
- **TWITCH_CLIENT_ID**: Registered app ID from Twitch Developer
- **TWITCH_CLIENT_SECRET**: Corresponding client secret
- **TWITCH_CHANNEL_ID**: Numeric ID of the Twitch channel to backup
- **USER_AUTH_TOKEN**: (Optional) User OAuth token for advanced access
- **USER_REFRESH_TOKEN**: (Optional) Refresh token for OAuth renewal
- **TELEGRAM_API_ID**: API ID from [my.telegram.org](https://my.telegram.org)
- **TELEGRAM_API_HASH**: API hash from [my.telegram.org](https://my.telegram.org)
- **TELEGRAM_PHONE_NUMBER**: Phone number associated with Telegram session
- **DATABASE_NAME**: SQLite database filename (e.g., `vods.db`)
- **TELEGRAM_CHANNELS**: Array of objects with `id` (channel ID starting with -) and `name` (channel description)
- **VOD_SETTINGS.MIN_RETRY_DELAY**: Minimum retry delay (seconds)
- **VOD_SETTINGS.MAX_RETRY_DELAY**: Maximum exponential backoff delay (seconds)
- **VOD_SETTINGS.AUTO_CHECK_VODS**: Flag to enable automatic VOD checking
- **VOD_SETTINGS.PHRASE_IN_THUMBNAIL_URL**: Array of phrases to identify processing VODs
- **VOD_SETTINGS.VIDEO_QUALITY_VOD**: Preferred video formats (priority order)
- **SPLIT_SETTINGS.MAX_FILE_SIZE_MB**: Maximum segment size (MB)
- **SPLIT_SETTINGS.MIN_FILE_SIZE_MB**: Minimum segment size (MB)
- **SPLIT_SETTINGS.MAX_RETRIES_SPLIT**: Maximum split attempts
- **SPLIT_SETTINGS.MERGE_THRESHOLD_MB**: Size threshold to merge final segments
- **SPLIT_SETTINGS.MID_TARGET_RATIO**: Intermediate target ratio between min/max
- **SPLIT_SETTINGS.UPLOAD_DELAY**: Delay (seconds) between consecutive uploads
- **SPLIT_SETTINGS.PROCESSING_INTERVAL**: Interval (seconds) for processing checks
- **SPLIT_SETTINGS.DROP_SEGMENT_THRESHOLD_SEC**: Minimum duration (seconds) to keep last segment
- **VOD_ORDERING.field**: Sorting field (`created_at` or `duration`)
- **VOD_ORDERING.order**: Sorting direction (`asc` or `desc`)


---

## 🚀 Installation & Setup

```bash
# Clone repository
git clone https://github.com/Daniele1926/Twitch-VODs-Backup-Telegram.git
cd Twitch-VODs-Backup-Telegram

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Configure config.json
# (See Configurazion section)

# Start script
python __main__.py
```

The script will start the Telegram client, initialize the database, and begin periodic VOD checks.

---

## 📊 Stato e log

- SQL database (`DATABASE_NAME`) contains `vods` and `operations` tables.
- Logs are saved to 'bot.log' with terminal progress bar display
- Critical errors are notified to configured Telegram channels

---

## 🤝 Contributing

1. Fork the project
2. Create a branch (`git checkout -b feature/nome-feature`)
3. Commit your changes 
4. Open a pull request

---

## 🙏 Credits

Code developed primarily with assistance from **ChatGPT** and **DeepSeek** for generation, refactoring, and testing

---


## 📄 License

Released under **AGPL-3.0** (GNU Affero General Public License v3.0). 

This means anyone can use, modify, and redistribute this software, but **if used to provide network-accessible services** (e.g., an online bot), modified code must be made publicly available.

Full details: [https://www.gnu.org/licenses/agpl-3.0.html](https://www.gnu.org/licenses/agpl-3.0.html)

