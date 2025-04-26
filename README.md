# Twitch-VODs-Backup-Telegram

Uno script Python asincrono per scaricare automaticamente i VOD (video on demand) da Twitch e inviarli su canali Telegram in modo continuo.

---

## 📦 Caratteristiche

- **Verifica e scarica nuovi VOD** da un canale Twitch specificato.
- **Gestione robusta dei download** con retry esponenziali e token refresh (user/app).
- **Splitting video** in segmenti di dimensione configurabile, con merge e ottimizzazione finale.
- **Correzione metadati** (faststart, moov atom) per streaming ottimale.
- **Upload su Telegram** tramite Telethon, con thumbnail generata automaticamente.
- **Database SQLite** per tenere traccia dello stato di ogni VOD (`pending`, `processing`, `live`, `completed`, `failed`).
- **File di log** centralizzato con integrazione `tqdm` per la progress bar.
- **Configurazione ricaricabile** a caldo senza riavviare lo script.

---

## 🛠️ Requisiti

- Python 3.8+
- ffmpeg e ffprobe installati e disponibili nel PATH
- `pip install -r requirements.txt`

**Dipendenze principali**:

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

## ⚙️ Configurazione

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

### Commenti di configurazione
- **TWITCH_CLIENT_ID**: ID dell'app registrata su Twitch Developer
- **TWITCH_CLIENT_SECRET**: Secret corrispondente al Client ID
- **TWITCH_CHANNEL_ID**: ID numerico del canale Twitch per cui fare il backup
- **USER_AUTH_TOKEN**: (Opzionale) Token OAuth di un utente per accesso avanzato
- **USER_REFRESH_TOKEN**: (Opzionale) Refresh token per rinnovare il token OAuth
- **TELEGRAM_API_ID**: API ID ottenuto da [my.telegram.org](https://my.telegram.org)
- **TELEGRAM_API_HASH**: API hash ottenuto da [my.telegram.org](https://my.telegram.org)
- **TELEGRAM_PHONE_NUMBER**: Numero di telefono associato alla sessione Telegram
- **DATABASE_NAME**: Nome del file SQLite (es. `vods.db`)
- **TELEGRAM_CHANNELS**: Array di oggetti con `id` (id del canale che inizia con -) e `name` (descrizione canale)
- **VOD_SETTINGS.MIN_RETRY_DELAY**: Ritardo minimo (s) tra tentativi di retry
- **VOD_SETTINGS.MAX_RETRY_DELAY**: Ritardo massimo (s) tra retry esponenziale
- **VOD_SETTINGS.AUTO_CHECK_VODS**: Flag per abilitare il controllo automatico dei VOD
- **VOD_SETTINGS.PHRASE_IN_THUMBNAIL_URL**: Array di frasi per identificare VOD in fase di elaborazione
- **VOD_SETTINGS.VIDEO_QUALITY_VOD**: Lista di formati preferiti (ordine di priorità)
- **SPLIT_SETTINGS.MAX_FILE_SIZE_MB**: Dimensione massima di ogni segmento (in MB)
- **SPLIT_SETTINGS.MIN_FILE_SIZE_MB**: Dimensione minima di ogni segmento (in MB)
- **SPLIT_SETTINGS.MAX_RETRIES_SPLIT**: Numero massimo di tentativi di split
- **SPLIT_SETTINGS.MERGE_THRESHOLD_MB**: Soglia di dimensione per unire segmenti finali
- **SPLIT_SETTINGS.MID_TARGET_RATIO**: Rapporto target intermedio tra min e max
- **SPLIT_SETTINGS.UPLOAD_DELAY**: Attesa (s) tra upload di segmenti consecutivi
- **SPLIT_SETTINGS.PROCESSING_INTERVAL**: Intervallo (s) per ripetere il ciclo di controllo
- **SPLIT_SETTINGS.DROP_SEGMENT_THRESHOLD_SEC**: Durata minima (s) per includere l'ultimo segmento
- **VOD_ORDERING.field**: Campo per ordinare i VOD (`created_at` o `duration`)
- **VOD_ORDERING.order**: Direzione ordinamento (`asc` o `desc`)


---

## 🚀 Installazione e avvio

```bash
# Clona il repository
git clone https://github.com/Daniele1926/Twitch-VODs-Backup-Telegram.git
cd Twitch-VODs-Backup-Telegram

# Installa le dipendenze
pip install --upgrade pip
pip install -r requirements.txt

# Configura il file config.json
# (vedi sezione Configurazione)

# Avvia lo script
python __main__.py
```

Lo script avvierà il client Telegram, inizializzerà il database e inizierà il controllo periodico dei VOD.

---

## 📊 Stato e log

- Il database SQLite (`DATABASE_NAME`) contiene le tabelle `vods` e `operations`.
- I log sono salvati in `bot.log` e mostrati a terminale con barra di progresso.
- Tutti gli errori critici vengono notificati sui canali Telegram configurati.

---

## 🤝 Contribuire

1. Fork del progetto
2. Crea un branch (`git checkout -b feature/nome-feature`)
3. Implementa e committa
4. Apri una Pull Request

---

## 🙏 Crediti

Il codice è stato sviluppato principalmente con l'aiuto di **ChatGPT** e **DeepSeek** per generazione, refactoring e testing

---


## 📄 Licenza

Questo progetto è rilasciato sotto licenza **AGPL-3.0** (GNU Affero General Public License v3.0). 

Ciò significa che chiunque può usare, modificare e ridistribuire questo software, ma **se il codice viene usato per offrire un servizio accessibile via rete (come un bot online)**, allora il codice modificato deve essere a sua volta reso disponibile al pubblico.

Per i dettagli completi: [https://www.gnu.org/licenses/agpl-3.0.html](https://www.gnu.org/licenses/agpl-3.0.html)

