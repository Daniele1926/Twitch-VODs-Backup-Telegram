import aiosqlite 
import aiohttp
import aiofiles 
import asyncio
from enum import Enum
import logging
import json
import errno
import stat
import random
import uuid
import os
import stat
import re
import yt_dlp 
import sys
import tempfile
import shutil
import signal
import cryptg
import time
import psutil
from datetime import datetime
from tqdm.asyncio import tqdm 
from telethon import TelegramClient 
from telethon.tl.types import DocumentAttributeVideo


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_ROOT = os.path.join(SCRIPT_DIR, "Temp")
LOG_FILE = os.path.join(SCRIPT_DIR, "bot.log")
if not os.path.exists(TEMP_ROOT):
    os.makedirs(TEMP_ROOT)

class VodStatus(str, Enum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    LIVE = 'live'
    COMPLETED = 'completed'
    FAILED = 'failed'

# Configurazione logging avanzata
# Configurazione logging avanzata
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg, end='\n')
        except Exception:
            self.handleError(record)

class RemoveUnwantedUpdatesFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        blocked_messages = [
            "Got difference for channel",
            "Got difference for account updates",
            "Server sent very old message with ID",
            "Security error while unpacking a received message"
        ]
        return not any(bm in msg for bm in blocked_messages)

def setup_logging():
    # Crea logger principale
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Rimuovi handlers esistenti
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Crea formattatore comune
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # File handler con encoding UTF-8
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Tqdm handler per output a schermo
    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setFormatter(formatter)
    root_logger.addHandler(tqdm_handler)

    # Applica filtri per Telethon
    telethon_logger = logging.getLogger("telethon.client.updates")
    telethon_logger.addFilter(RemoveUnwantedUpdatesFilter())
    telethon_logger.setLevel(logging.WARNING)

    # Imposta livello per altri logger Telethon
    for name in ["telethon", "telethon.network", "telethon.crypto"]:
        logging.getLogger(name).setLevel(logging.WARNING)

# Inizializza il logging all'avvio
setup_logging()
logger = logging.getLogger(__name__)

class ReloadableConfig:
    def __init__(self, path):
        self.path = path
        self.config = {}
        self.last_modified = 0
        self.load_sync()

    def load_sync(self):
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            self.last_modified = os.path.getmtime(self.path)
            logger.info("Configurazione caricata con successo")
        except Exception as e:
            logger.critical(f"Errore caricamento config: {str(e)}")
            sys.exit(1)

    async def reload(self):
        try:
            mod_time = os.path.getmtime(self.path)
            if mod_time > self.last_modified:
                logger.info("Rilevata modifica del file di configurazione, ricarico...")
                async with aiofiles.open(self.path, mode='r') as f:
                    content = await f.read()
                    self.config = json.loads(content)
                    self.last_modified = mod_time
                logger.info("Configurazione ricaricata")
                return True
            return False
        except Exception as e:
            logger.error(f"Errore ricaricamento config: {str(e)}")
            return False
    
    def __getitem__(self, key):
        return self.config[key]
    
    def get(self, key, default=None):
        return self.config.get(key, default)

class TwitchTokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None
        
    async def refresh_token(self):
        logger.info("Refreshing Twitch token...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://id.twitch.tv/oauth2/token",
                    params={
                        "client_id": config["TWITCH_CLIENT_ID"],
                        "client_secret": config["TWITCH_CLIENT_SECRET"],
                        "grant_type": "client_credentials"
                    }
                ) as response:
                    data = await response.json()
                    if 'access_token' not in data or 'expires_in' not in data:
                        raise KeyError("Response missing required token fields")
                    self.token = data["access_token"]
                    self.expires_at = time.time() + data["expires_in"] - 300
                    logger.info(f"Twitch token refreshato. Scadenza: {self.expires_at}")
        except (KeyError, json.JSONDecodeError) as e:
            logger.critical(f"Errore update token: {str(e)}")
            raise

    async def get_valid_token(self):
        if not self.token or time.time() > self.expires_at:
            await self.refresh_token()
        return self.token

try:
    config = ReloadableConfig("config.json")
    token_manager = TwitchTokenManager()
except Exception as e:
    logger.critical(f"Errore di configurazione: {e}")
    sys.exit(1)

class NetworkRetryManager:
    def __init__(self):
        self.last_failure = None
        self.consecutive_errors = 0
    
    async def should_retry(self, exception):
        if isinstance(exception, (aiohttp.ClientConnectionError, asyncio.TimeoutError)):
            self.consecutive_errors += 1
            wait_time = min(2 ** self.consecutive_errors, 300)
            logger.warning(f"Network error: {exception}. Retrying in {wait_time} seconds (attempt {self.consecutive_errors}).")
            await asyncio.sleep(wait_time)
            return True
        logger.info(f"Non-network error, reset contatore retry: {type(exception).__name__}")
        self.consecutive_errors = 0
        return False
    
class ProgressBarManager:
    """Gestione centralizzata delle progress bar con tqdm"""
    def __init__(self):
        self.bars = {}
        self.lock = asyncio.Lock()
        self.next_position = 0

    async def create_bar(self, bar_id, desc, total, unit='MB'):
        async with self.lock:
            if bar_id in self.bars:
                self.bars[bar_id].close()
            
            new_bar = tqdm(
                desc=desc,
                total=total,
                unit=unit,
                unit_scale=True,
                dynamic_ncols=True,
                leave=False,
                position=self.next_position,
                mininterval=0.1,  # <-- Forza aggiornamenti più frequenti
                maxinterval=1.0,
                ascii=True
            )
            self.bars[bar_id] = new_bar
            self.next_position += 1

    async def update_bar(self, bar_id, value):
        async with self.lock:
            if bar_id in self.bars:
                self.bars[bar_id].n = min(value, self.bars[bar_id].total)
                self.bars[bar_id].refresh()

    async def close_bar(self, bar_id):
        async with self.lock:
            if bar_id in self.bars:
                self.bars[bar_id].close()
                del self.bars[bar_id]
                self.next_position = max(0, self.next_position - 1)

    async def close_all(self):
        async with self.lock:
            for bar in self.bars.values():
                bar.close()
            self.bars.clear()
            self.next_position = 0

progress_manager = ProgressBarManager()

async def check_dependencies():
    required_tools = ['ffprobe', 'ffmpeg']
    logger.info(f"Verifico dipendenze di sistema: {', '.join(required_tools)}")
    missing = []
    for tool in required_tools:
        if shutil.which(tool) is None:
            missing.append(tool)
    if missing:
        logger.critical(f"Strumenti mancanti: {', '.join(missing)}")
        raise RuntimeError(f"Strumenti mancanti: {', '.join(missing)}")
    logger.info("Tutte le dipendenze sono installate")
    
client = TelegramClient("session", config["TELEGRAM_API_ID"], config["TELEGRAM_API_HASH"])

async def init_db():
    logger.info("Inizializzazione database...")  
    async with aiosqlite.connect(config["DATABASE_NAME"]) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS vods (
                id TEXT PRIMARY KEY, 
                title TEXT, 
                created_at TEXT,
                duration INTEGER,
                duration_str TEXT,
                status TEXT DEFAULT 'pending',
                retries INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                next_retry TIMESTAMP,
                last_checked TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vod_id TEXT,
                operation_type TEXT,
                status TEXT,
                timestamp DATETIME,
                details TEXT,
                FOREIGN KEY(vod_id) REFERENCES vods(id)
            )
        ''')
        await db.commit()
    logger.info("Database inizializzato")

async def log_operation(vod_id, op_type, status, details=""):
    try:
        logger.info(f"Logging operazione: {vod_id} - {op_type} - {status}")  
        async with aiosqlite.connect(config["DATABASE_NAME"]) as db:
            await db.execute('''
                INSERT INTO operations 
                (vod_id, operation_type, status, timestamp, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (vod_id, op_type, status, datetime.now().isoformat(), details))
            await db.commit()
        logger.info(f"Log operazione: {vod_id} - {op_type} - {status}")
    except Exception as e:
        logger.error(f"Fallito log operazione: {str(e)}")
        raise

async def update_vod_status(vod_id, status, increment_retries=False):
    logger.info(f"Aggiornamento stato VOD {vod_id} a {status} (incrementa retries: {increment_retries})") 
    async with aiosqlite.connect(config["DATABASE_NAME"]) as db:
        try:
            if increment_retries:
                cursor = await db.execute("SELECT retries FROM vods WHERE id = ?", (vod_id,))
                result = await cursor.fetchone()
                retries = result[0] if result else 0
                
                vod_settings = config["VOD_SETTINGS"]
                min_retry_delay = vod_settings.get("MIN_RETRY_DELAY", 300)
                max_retry_delay = vod_settings.get("MAX_RETRY_DELAY", 86400)
                delay = min(min_retry_delay * (2 ** retries), max_retry_delay)
                next_retry = datetime.fromtimestamp(time.time() + delay).isoformat()

                logger.warning(f"Aumento tentativi per {vod_id} (nuovo tentativo in {delay}s)")  
                await db.execute('''
                    UPDATE vods 
                    SET status = ?, retries = retries + 1, 
                        last_attempt = ?, next_retry = ?
                    WHERE id = ?
                ''', (status, datetime.now().isoformat(), next_retry, vod_id))
            else:
                await db.execute('''
                    UPDATE vods 
                    SET status = ?, last_attempt = ?
                    WHERE id = ?
                ''', (status, datetime.now().isoformat(), vod_id))

            await db.commit()
            logger.info(f"Update stato per {vod_id}: {status}")  
        except Exception as e:
            logger.error(f"Update status fallito per {vod_id}: {str(e)}")  
            await db.rollback()
            raise

async def get_pending_vods():
    logger.info("Recupero pending VODs...")
    
    ordering_config = config.get("VOD_ORDERING", {"field": "created_at", "order": "asc"})
    field = ordering_config.get("field", "created_at")
    order = ordering_config.get("order", "asc").upper()
    
    query = f"""
        SELECT * FROM vods 
        WHERE status = ? 
          AND (next_retry IS NULL OR datetime(next_retry) <= datetime('now'))
          AND status NOT IN ('{VodStatus.LIVE.value}', '{VodStatus.FAILED.value}', '{VodStatus.COMPLETED.value}')
        ORDER BY {field} {order}, retries ASC
        LIMIT 100
    """
    
    async with aiosqlite.connect(config["DATABASE_NAME"]) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(query, (VodStatus.PENDING.value,))
        rows = await cursor.fetchall()
        results = [dict(row) for row in rows]
        logger.info(f"Trovati {len(results)} pending VODs ordinati per {field} {order}")
        return results

async def log_vod_stats():
    async with aiosqlite.connect(config["DATABASE_NAME"]) as db:
        cursor = await db.execute("SELECT status, COUNT(*) as count FROM vods GROUP BY status")
        rows = await cursor.fetchall()
        stats = { row[0]: row[1] for row in rows }
        logger.info(
            f"VOD Stats: Completed: {stats.get(VodStatus.COMPLETED.value, 0)}, "
            f"Pending: {stats.get(VodStatus.PENDING.value, 0)}, "
            f"Processing: {stats.get(VodStatus.PROCESSING.value, 0)}, "
            f"Failed: {stats.get(VodStatus.FAILED.value, 0)}, "
            f"Live: {stats.get(VodStatus.LIVE.value, 0)} "
        )

async def handle_failed_vod(vod_id):
    logger.error(f"VOD {vod_id} ha fallito dopo il massimo numero di tentativi.")
    try:
        await send_telegram_notification(f"⛔ VOD {vod_id} failed after 5 attempts.")
        await update_vod_status(vod_id, VodStatus.FAILED.value, increment_retries=False)
    except Exception as e:
        logger.critical(f"Critical error in failure handler: {str(e)}")

def parse_duration(duration_str):
    total_seconds = 0
    match = re.match(r'((?P<hours>\d+)h)?((?P<minutes>\d+)m)?((?P<seconds>\d+)s)?', duration_str)
    if match:
        time_data = match.groupdict(default="0")
        total_seconds = int(time_data["hours"]) * 3600 + int(time_data["minutes"]) * 60 + int(time_data["seconds"])
    return total_seconds

async def check_new_vods():
    logger.info("Check nuovi VODs su Twitch...")
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": config["TWITCH_CLIENT_ID"],
                "Authorization": f"Bearer {await token_manager.get_valid_token()}"
            }
            params = {
                "user_id": config["TWITCH_CHANNEL_ID"],
                "type": "archive",
                "first": 100
            }
            async with session.get(
                "https://api.twitch.tv/helix/videos",
                headers=headers,
                params=params
            ) as response:
                if response.status != 200:
                    logger.error(f"Twitch API error: {response.status}")
                    return
                
                data = await response.json()
                new_vods = 0
                live_phrases = config.get("PHRASE_IN_THUMBNAIL_URL", ["404_processing"])
                
                async with aiosqlite.connect(config["DATABASE_NAME"]) as db:
                    db.row_factory = aiosqlite.Row
                    for vod in data['data']:
                        duration_str = vod.get("duration", "0s")
                        duration_seconds = parse_duration(duration_str)
                        
                        is_live = any(phrase in vod['thumbnail_url'] for phrase in live_phrases)
                        status = VodStatus.LIVE if is_live else VodStatus.PENDING
                        
                        cursor = await db.execute("SELECT status FROM vods WHERE id = ?", (vod['id'],))
                        existing = await cursor.fetchone()
                        
                        if existing and existing['status'] in (VodStatus.COMPLETED.value, VodStatus.FAILED.value):
                            continue
                        
                        if not existing:
                            await db.execute('''
                                INSERT INTO vods 
                                (id, title, created_at, duration, duration_str, status)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (
                                vod['id'],
                                vod['title'],
                                vod['created_at'],
                                duration_seconds,
                                duration_str,
                                status.value
                            ))
                            new_vods += 1
                            logger.info(f"Nuovo VOD trovato: {vod['id']} - Status: {status.value}")
                        else:
                            if existing['status'] == VodStatus.LIVE.value and not is_live:
                                await db.execute('''
                                    UPDATE vods 
                                    SET status = ?, duration = ?, duration_str = ?
                                    WHERE id = ?
                                ''', (VodStatus.PENDING.value, duration_seconds, duration_str, vod['id']))
                                logger.info(f"VOD {vod['id']} ora disponibile, aggiornato a pending")
                            elif existing['status'] != status.value:
                                await db.execute('''
                                    UPDATE vods 
                                    SET status = ?, duration = ?, duration_str = ?
                                    WHERE id = ?
                                ''', (status.value, duration_seconds, duration_str, vod['id']))
                                logger.info(f"Aggiornato VOD {vod['id']} status a {status.value}")
                    
                    await db.commit()
                
                logger.info(f"Aggiunti {new_vods} nuovi VODs")
    except Exception as e:
        logger.error(f"Errore controllo nuovi VODs: {str(e)}")

async def validate_media_file(file_path):
    logger.info(f"Validazione media file: {file_path}")
    progress_bar = None
    try:
        metadata = await get_video_metadata(file_path)
        total_duration = float(metadata['duration'])

        progress_bar = tqdm(
            total=total_duration, 
            desc="Validating media", 
            unit="s", 
            unit_scale=True, 
            dynamic_ncols=True, 
            leave=False
        )

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-i", file_path,
            "-f", "null", "-",
            "-progress", "pipe:1"
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

        time_pattern = re.compile(r"out_time_us=(\d+)")
        start_time = time.time()

        async def read_stdout():
            last_update = 0
            async for line in proc.stdout:
                line = line.decode().strip()
                match = time_pattern.search(line)
                if match:
                    current_time = int(match.group(1)) / 1_000_000
                    progress_bar.n = min(current_time, total_duration)
                    
                    if time.time() - last_update > 0.5:
                        progress_bar.refresh()
                        last_update = time.time()

        stdout_reader = asyncio.create_task(read_stdout())
        await proc.wait()
        await stdout_reader

        if proc.returncode != 0:
            logger.error(f"Validazione fallita con codice {proc.returncode}")
            raise Exception(f"Validation failed with code {proc.returncode}")

        progress_bar.n = total_duration
        progress_bar.refresh()
        logger.info("Media file validato")
        return True
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        raise
    finally:
        if progress_bar is not None:
            progress_bar.close()
    
async def send_telegram_notification(message):
    logger.info(f"Invio notifica Telegram: {message}")
    try:
        await client.send_message(
            config["TELEGRAM_CHANNEL_ID"],
            f"**System Notification**\n{message}",
            parse_mode='md'
        )
    except Exception as e:
        logger.error(f"Invio notifica fallito: {str(e)}")

def kill_child_processes():
    """Terminazione forzata processi figli con supporto Windows"""
    try:
        current_process = psutil.Process()
        children = current_process.children(recursive=True)
        for child in children:
            try:
                child.kill()
                if os.name == 'nt':  # Forza terminazione immediata su Windows
                    os.system(f'taskkill /F /PID {child.pid}')
            except psutil.NoSuchProcess:
                pass
    except Exception as e:
        logger.error(f"Errore terminazione processi: {str(e)}")

def on_rmtree_error(func, path, exc_info):
    """
    Gestore avanzato degli errori per shutil.rmtree con:
    - Gestione ricorsiva dei permessi
    - Tentativi multipli intelligenti
    - Differenziazione errori Windows/POSIX
    """
    error_detail = exc_info[1] if isinstance(exc_info, tuple) else exc_info
    logger.error(f"⚠️ Errore rimozione {path} [{func.__name__}]: {str(error_detail)}")

    # Mappatura avanzata errori
    error_mapping = {
        errno.EACCES: ("Permesso negato", "File/directory bloccato", 0o777),
        errno.EPERM: ("Operazione non permessa", "Mancano i privilegi", stat.S_IWUSR),
        errno.ENOENT: ("File non trovato", "Path inesistente", None),
        errno.EBUSY: ("File in uso", "Risorsa occupata", None),
        errno.ENOTEMPTY: ("Directory non vuota", "Contenuto residuo", None)
    }

    # Correzione: aggiunto parentesi mancante
    error_code = getattr(error_detail, 'winerror', getattr(error_detail, 'errno', None))  # <-- FIX HERE
    
    # Ottieni dettaglio errore
    error_msg, solution, perm = error_mapping.get(error_code, ("Errore sconosciuto", "Nessuna soluzione nota", None))
    logger.warning(f"💡 Diagnostica: {error_msg} | 💡 Soluzione: {solution}")

    max_retries = 7 if os.name == 'nt' else 5
    for attempt in range(1, max_retries + 1):
        try:
            # Fix permessi mirato
            if perm:
                if os.path.isdir(path):
                    os.chmod(path, perm)
                    if os.name == 'nt':
                        os.chmod(path, stat.S_IRWXU)
                else:
                    os.chmod(path, perm)

            # Forza rimozione ricorsiva per directory
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=False, onerror=on_rmtree_error)
            else:
                os.remove(path)

            logger.info(f"✅ Rimosso con successo al tentativo {attempt}")
            return

        except Exception as e:
            logger.warning(f"⚙️ Tentativo {attempt}/{max_retries} fallito: {str(e)}")
            time.sleep(2 ** attempt + random.uniform(0, 1))  # Backoff esponenziale con jitter

    logger.critical(f"🚨 Rimozione fallita dopo {max_retries} tentativi per: {path}")

# Correzione nella funzione cleanup_temp_files
def cleanup_temp_files(file_dir):
    """
    Pulizia avanzata delle directory temporanee con:
    - Gestione ricorsiva dei permessi
    - Tentativi adattivi
    - Pulizia atomica
    """
    logger.info(f"🚮 Avvio pulizia avanzata per: {file_dir}")
    
    if not os.path.exists(file_dir):
        logger.warning("⌦ Directory inesistente, operazione annullata")
        return True

    # Fase 1: Normalizzazione permessi ricorsiva
    def fix_permissions(root_path):
        for root, dirs, files in os.walk(root_path, topdown=False):
            for name in dirs + files:
                path = os.path.join(root, name)
                try:
                    if os.name == 'nt':
                        os.chmod(path, stat.S_IRWXU)
                    else:
                        os.chmod(path, 0o777)
                except Exception as e:
                    logger.error(f"⚙️ Impostazione permessi fallita per {path}: {str(e)}")

    fix_permissions(file_dir)

    # Fase 2: Rimozione adattiva
    max_retries = 5
    retry_delay = 1.5

    for attempt in range(max_retries):
        try:
            logger.info(f"♻️ Tentativo {attempt + 1}/{max_retries}")
            shutil.rmtree(file_dir, onexc=on_rmtree_error)
            break  # Uscita anticipata se successo
        except Exception as e:
            logger.warning(f"🔥 Tentativo {attempt + 1} fallito: {str(e)}")
            fix_permissions(file_dir)  # Ripristina permessi
            time.sleep(retry_delay)
            retry_delay *= 2.5  # Backoff esponenziale

    # Verifica incrociata
    if os.path.exists(file_dir):
        logger.critical(f"🚨 Directory residua: {file_dir}")
        logger.critical("📁 Contenuto finale: " + str(os.listdir(file_dir)))
        return False
    
    logger.info("✅ Pulizia completata con successo")
    return True

# --- Download, split, upload e gestione errori ---
async def download_vod(vod, token_manager):
    logger.info(f"Avvio download per VOD {vod['id']} - {vod['title']}")
    vod_dir = os.path.join(TEMP_ROOT, vod['id'])
    os.makedirs(vod_dir, exist_ok=True)
    filename = os.path.join(vod_dir, f"{vod['id']}.mp4")

    original_sigint = original_sigterm = None
    download_success = False
    using_user_token = False

    def handle_signal(signal_num, frame):
        try:
            logger.warning("Segnale interrupt ricevuto! Pulisco...")
            kill_child_processes()  # Aggiungi questa linea
            cleanup_temp_files(vod_dir)
        except Exception as e:
            logger.critical(f"CRITICAL ERROR IN SIGNAL HANDLER: {str(e)}")
        finally:
            sys.exit(1)

    try:
        original_sigint = signal.signal(signal.SIGINT, handle_signal)
        original_sigterm = signal.signal(signal.SIGTERM, handle_signal)

        # Costruzione header di autenticazione
        user_auth_token = config.get("USER_AUTH_TOKEN")
        auth_header = None
        if user_auth_token:
            user_auth_token = user_auth_token.replace('oauth:', '', 1)  # Rimuove prefisso legacy se presente
            auth_header = f'Bearer {user_auth_token}'
            using_user_token = True
            logger.info("Utilizzo user token per l'autenticazione")
        else:
            app_token = await token_manager.get_valid_token()
            auth_header = f'Bearer {app_token}'
            logger.info("Utilizzo app token per l'autenticazione")

        ydl_opts = {
            'cookiefile': 'cookies.txt',
            'format': '/'.join(config["VOD_SETTINGS"]["VIDEO_QUALITY_VOD"]),
            'outtmpl': filename,
            'http_headers': {
                'Authorization': auth_header,
                'Client-ID': config["TWITCH_CLIENT_ID"]
            },
            'concurrent_fragment_downloads': 10,
            'break_on_reject': False,
            'quiet': True,
            'ratelimit': config["VOD_SETTINGS"].get("DOWNLOAD_RATELIMIT", 10000000),
            'retries': config["VOD_SETTINGS"].get("MAX_RETRIES_DOWNLOAD", 10),
            'skip_unavailable_fragments': False,
            'force_keyframes_at_cuts': True,
            'fragment_retries': 100,
            'hls_use_mpegts': True
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info("Fetching video metadata...")
            meta_info = await asyncio.to_thread(
                ydl.extract_info,
                f"https://www.twitch.tv/videos/{vod['id']}",
                download=False
            )

            if not meta_info.get('formats'):
                raise Exception("No formats available for download")

            selected_format = next(
                (f for f in meta_info['formats'] if f['format_id'] == meta_info['format_id']),
                None
            )
            if selected_format:
                logger.info(
                    f"Formato selezionato: {selected_format.get('format_id', 'format_note')} "
                    f"({selected_format.get('width', '?')}x{selected_format.get('height', '?')}) "
                    f"@{selected_format.get('tbr', 0):.2f}kbps "
                    f"{meta_info.get('duration')/3600:.2f} ore"
                )

            logger.info(f"Avvio download in: {filename}")
            start_time = time.time()
            info = await asyncio.to_thread(
                ydl.extract_info,
                f"https://www.twitch.tv/videos/{vod['id']}",
                download=True
            )
            final_filename = ydl.prepare_filename(info)

            logger.info("Avvio correzione metadati post-download...")
            try:
                final_filename = await fix_metadata(final_filename)
                fixed_meta = await get_video_metadata(final_filename)
                logger.info(f"Metadati fissati | Durata: {fixed_meta['duration']:.2f}s")
            except Exception as meta_error:
                logger.error(f"Errore correzione metadati: {str(meta_error)}")
                raise
                        
            file_size = os.path.getsize(final_filename)
            logger.info(
                f"Download completato in {time.time() - start_time:.2f}s | "
                f"Size: {file_size/1000/1000:.2f}MB | "
                f"Bitrate: {(file_size * 8) / (float(info['duration']) * 1000):.2f}kbps"
            )
            download_success = True
            return final_filename

    except yt_dlp.DownloadError as e:
        error_msg = str(e)
        logger.error(f"Download fallito per VOD {vod['id']}: {error_msg}")
        if '401' in error_msg or 'Unauthorized' in error_msg:
            if using_user_token:
                logger.error("User token non valido o scaduto. Verifica il token in config.json.")
                await log_operation(vod['id'], 'download', 'failed', "Invalid or expired user token")
            else:
                logger.error("Accesso negato. Il VOD potrebbe essere disponibile solo per abbonati. Utilizza un user token in config.json.")
                await log_operation(vod['id'], 'download', 'failed', "Subscriber-only VOD requires user auth")
        else:
            await log_operation(vod['id'], 'download', 'failed', error_msg)
        raise
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("Download interrotto! Avvio pulizia...")
        await log_operation(vod['id'], 'download', 'interrupted')
        raise
    except Exception as e:
        logger.error("Eccezione durante il download", exc_info=True)
        await log_operation(vod['id'], 'download', 'error', str(e))
        raise
    finally:
        if original_sigint is not None:
            signal.signal(signal.SIGINT, original_sigint)
        if original_sigterm is not None:
            signal.signal(signal.SIGTERM, original_sigterm)
        if not download_success:
            logger.info("Avvio pulizia finale a causa di errore nel download...")
            try:
                cleanup_temp_files(vod_dir)
                logger.info("Pulizia completata")
            except Exception as cleanup_error:
                logger.error(f"Errore durante la pulizia: {cleanup_error}")

async def handle_vod_error(vod, error):
    vod_id = vod['id']
    logger.error(f"Errore elaborazione VOD {vod_id}: {str(error)}", exc_info=True)
    # Aggiorno lo stato a pending (con incremento tentativi)
    await update_vod_status(vod_id, VodStatus.PENDING.value, increment_retries=True)
    max_retries = 10
    # Se i retry raggiungono il massimo, imposta a FAILED
    if vod['retries'] + 1 >= max_retries:
        logger.critical(f"Raggiunto massimo tentativi per VOD {vod_id}")
        await handle_failed_vod(vod_id)

async def cleanup_vod(vod_id, vod_dir):
    """Utilizza la stessa logica robusta di cleanup_temp_files"""
    logger.info(f"Pulizia directory VOD {vod_id}")
    return cleanup_temp_files(vod_dir)

# -------------------------------------------------------------------------------
# FUNZIONE: get_video_metadata
# -------------------------------------------------------------------------------
async def get_video_metadata(input_path):
    """
    Estrae i metadati del video utilizzando ffprobe e restituisce un dizionario.
    La struttura JSON di output contiene le informazioni relative a width, height,
    duration, bit_rate e file_size.
    """
    logger.info(f"Estrazione metadata per: {input_path}")
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration,bit_rate",
            "-of", "json",
            input_path
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffprobe error: {stderr.decode()}")
        data = json.loads(stdout)
        if not data.get('streams'):
            raise Exception("Nessuno stream video trovato")
        stream = data['streams'][0]
        width = stream.get('width', 0)
        height = stream.get('height', 0)
        duration = float(stream.get('duration', 0))
        bit_rate = int(stream.get('bit_rate', 0))
        file_size = os.path.getsize(input_path)
        if duration <= 0:
            logger.warning("Durata non trovata da ffprobe")
            duration = await get_segment_duration(input_path)
        if bit_rate <= 0 and duration > 0:
            bit_rate = int((file_size * 8) / duration)
        metadata = {
            'width': width,
            'height': height,
            'duration': duration,
            'bit_rate': bit_rate,
            'file_size': file_size
        }
        logger.info(f"Metadata ottenuti: {width}x{height} | {duration:.2f}s | {bit_rate/1000:.2f}kbps | {file_size/1000**2:.2f}MB")
        return metadata
    except Exception as e:
        logger.error(f"Errore estrazione metadata: {str(e)}")
        raise

# -------------------------------------------------------------------------------
# FUNZIONE: get_segment_duration
# -------------------------------------------------------------------------------
async def get_segment_duration(segment_path):
    """
    Estrae la durata del segmento (in secondi) utilizzando ffprobe.
    """
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        segment_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe error: {stderr.decode()}")
    try:
        return float(stdout.decode().strip())
    except ValueError:
        raise RuntimeError("Formato durata non valido")

# -------------------------------------------------------------------------------
# FUNZIONE: fix_metadata
# -------------------------------------------------------------------------------
async def fix_metadata(input_file):
    logger.info(f"Fixing metadata per: {os.path.basename(input_file)}")
    temp_file = input_file.replace(".mp4", "_fixed.mp4")

    cmd = [
        "ffmpeg",
        "-y", 
        "-loglevel", "info",  # Medio tra performance e debugging
        "-probesize", "5M",     # Sufficiente per il probing iniziale
        "-analyzeduration", "5M", # Analisi minima ma sicura
        "-i", input_file,
        "-c", "copy",
        "-movflags", "+faststart", 
        "-max_muxing_queue_size", "9999",
        "-ignore_unknown",
        "-fflags", "+nobuffer",
        "-f", "mp4",
        "-progress", "pipe:1",
        "-nostdin",
        temp_file
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )

    duration_pattern = re.compile(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)")
    out_time_pattern = re.compile(r"out_time=(\d+):(\d+):(\d+\.\d+)")

    progress_bar = tqdm(
        total=100,
        desc="Elaborazione video",
        unit="%",
        ascii=True,
        bar_format="{l_bar}{bar}| {n:.1f}% [{elapsed}<{remaining}]",
        leave=False
    )

    async def monitor_progress():
        buffer = ""
        duration = None
        last_progress = 0.0

        try:
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                buffer += chunk.decode(errors='ignore')
                
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    
                    # Debug logging per output FFmpeg
                    logger.debug(f"FFmpeg: {line.strip()}")
                    
                    # Estrazione durata
                    if not duration:
                        match_duration = duration_pattern.search(line)
                        if match_duration:
                            h, m, s = map(float, match_duration.groups())
                            duration = h * 3600 + m * 60 + s
                            progress_bar.reset(total=100)
                            logger.debug(f"Durata rilevata: {duration}s")
                    
                    # Calcolo progresso
                    match_progress = out_time_pattern.search(line)
                    if match_progress and duration:
                        h, m, s = map(float, match_progress.groups())
                        current_time = h * 3600 + m * 60 + s
                        progress = (current_time / duration) * 100
                        progress_bar.update(progress - last_progress)
                        last_progress = progress
                        logger.debug(f"Progresso: {progress:.1f}%")

        except Exception as e:
            logger.error(f"Errore durante il monitoraggio: {str(e)}")
            raise
        finally:
            # Aspetta la terminazione del processo in ogni caso
            await proc.wait()
            logger.debug(f"Processo terminato con codice: {proc.returncode}")

            # Aggiornamento finale solo se completato con successo
            if proc.returncode == 0:
                progress_bar.n = 100
                progress_bar.refresh()
            progress_bar.close()

    try:
        await asyncio.wait_for(monitor_progress(), timeout=7200)
    except asyncio.TimeoutError:
        logger.error("Timeout superato (2 ore)")
        proc.kill()
        await proc.wait()
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise RuntimeError("Timeout di elaborazione")

    # Controllo finale del returncode
    if proc.returncode != 0:
        error_log = f"FFmpeg fallito con codice {proc.returncode}"
        logger.error(error_log)
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise RuntimeError(error_log)

    # Sostituzione atomica del file
    os.replace(temp_file, input_file)
    return input_file

# -------------------------------------------------------------------------------
# FUNZIONE: merge_segments
# -------------------------------------------------------------------------------
async def merge_segments(segments):
    """
    Unisce i segmenti passati come lista e restituisce il percorso del file unito.
    Utilizza il metodo 'concat' di ffmpeg e, in caso di merge riuscito,
    elimina i segmenti originali.
    """
    logger.info(f"Avvio merging di {len(segments)} segmenti")
    merged_file = segments[0].replace("_part", "_merged_")
    logger.debug(f"File merged: {merged_file}")
    
    # Crea il file di concordanza
    with open("concat.txt", "w") as f:
        for s in segments:
            f.write(f"file '{s}'\n")
    
    cmd = [
        "ffmpeg", "-f", "concat", "-safe", "0",
        "-i", "concat.txt", "-c", "copy",
        "-movflags", "+faststart",
        "-y", merged_file
    ]
    
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await asyncio.wait_for(proc.communicate(), timeout=1800)
        
        if os.path.exists(merged_file) and os.path.getsize(merged_file) > 0:
            # Fissa i metadati del file merged
            merged_size = os.path.getsize(merged_file)
            logger.info(f"Merge completato: {os.path.basename(merged_file)} - {merged_size/1000**2:.2f}MB")
            
            # Elimina i segmenti originali e il file di concordanza
            logger.debug("Eliminazione segmenti originali:")
            for s in segments:
                os.remove(s)
                logger.debug(f"   Eliminato: {os.path.basename(s)}")
            os.remove("concat.txt")
            
            return merged_file
        else:
            raise Exception("Merge failed: output file not created")
            
    except Exception as e:
        logger.error(f"Merge fallito: {str(e)}")
        return None

# -------------------------------------------------------------------------------
# FUNZIONE: process_pending_merge
# -------------------------------------------------------------------------------
async def process_pending_merge(pending, max_size, merge_threshold):
    """
    Prova a unire i segmenti presenti in pending; se il merge ha successo,
    restituisce il file unito, altrimenti restituisce i segmenti originali.
    """
    valid_segments = [s for s in pending if os.path.exists(s)]
    if len(valid_segments) <= 1:
        return valid_segments

    total_size = sum(os.path.getsize(s) for s in valid_segments)
    logger.info(f"Tentativo merge di {len(valid_segments)} segmenti (dimensione totale: {total_size/1e6:.2f}MB)")
    
    if total_size <= max_size:
        try:
            merged = await merge_segments(valid_segments)
            logger.info(f"Merge riuscito: {os.path.basename(merged)}")
            return [merged]
        except Exception as e:
            logger.error(f"Merge fallito: {str(e)}")
            return valid_segments
    else:
        logger.info("ℹ️ Dimensione totale supera il massimo, segmenti mantenuti separati")
        return valid_segments

# -------------------------------------------------------------------------------
# FUNZIONE: optimize_segments
# -------------------------------------------------------------------------------
async def optimize_segments(segments, max_size, merge_threshold, min_size):
    """
    Ottimizza i segmenti ottenuti:
    - Merge solo tra ultimo e penultimo se:
        1. L'ultimo segmento è < merge_threshold OPPURE < min_size
        2. La somma delle dimensioni è <= max_size
    """
    logger.info("Ottimizzazione segmenti...")
    optimized = segments.copy()

    if len(optimized) >= 2:
        last_seg = optimized[-1]
        penultimate_seg = optimized[-2]
        last_size = os.path.getsize(last_seg)
        penultimate_size = os.path.getsize(penultimate_seg)
        
        logger.info(
            f"Analisi finale - Ultimo: {os.path.basename(last_seg)} ({last_size/1e6:.2f}MB), "
            f"Penultimo: {os.path.basename(penultimate_seg)} ({penultimate_size/1e6:.2f}MB)"
        )

        # Condizioni aggiornate con OR per i trigger e controllo dimensione
        trigger_condition = (last_size < merge_threshold) and (last_size < min_size)
        size_condition = (penultimate_size + last_size) <= max_size
        
        if trigger_condition and size_condition:
            logger.info("Tentativo merge ultimi due segmenti...")
            try:
                merged_candidate = await merge_segments([penultimate_seg, last_seg])
                optimized = optimized[:-2] + [merged_candidate]
                logger.info(f"Merge riuscito: {os.path.basename(merged_candidate)}")
            except Exception as e:
                logger.error(f"Merge fallito: {str(e)}")
        else:
            reason = []
            if not trigger_condition: reason.append("trigger non soddisfatto")
            if not size_condition: reason.append(f"somma {(penultimate_size + last_size)/1e6:.2f}MB > {max_size/1e6:.2f}MB")
            logger.info(f"ℹ️ Merge non eseguito: {' + '.join(reason)}")
    
    return [s for s in optimized if os.path.exists(s)]

async def generate_thumbnail(video_path):
    """Genera una thumbnail a metà durata del video"""
    logger.info(f"Generazione thumbnail per {video_path}")
    output_path = os.path.splitext(video_path)[0] + "_thumb.jpg"
    
    try:
        metadata = await get_video_metadata(video_path)
        duration = float(metadata['duration'])
        
        # Calcola il punto medio con controllo durata minima
        seek_time = max(duration / 2, 1)  # Almeno 1 secondo per video molto corti
        
        cmd = [
            "ffmpeg",
            "-y",
            "-ss", str(seek_time),
            "-i", video_path,
            "-vframes", "1",
            "-q:v", "2",  # Qualità JPEG (2-31, 2=migliore)
            "-vf", "scale=320:-1",  # Ridimensiona a 320px di larghezza
            output_path
        ]
        
        proc = await asyncio.create_subprocess_exec(*cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            raise Exception(f"Thumbnail generation failed: {stderr.decode()}")
            
        if not os.path.exists(output_path):
            raise Exception("File thumbnail non creato")
            
        return output_path
        
    except Exception as e:
        logger.error(f"Errore generazione thumbnail: {str(e)}")
        raise

# -------------------------------------------------------------------------------
# FUNZIONE: split_video
# -------------------------------------------------------------------------------
async def check_encoder_available(encoder_name):
    """Verifica la disponibilità di un encoder specifico."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-encoders",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        # Combina stdout e stderr, poiché ffmpeg può utilizzare entrambi per il comando -encoders
        output = stdout.decode("utf-8", errors="ignore") + stderr.decode("utf-8", errors="ignore")
        return encoder_name.lower() in output.lower()
    except Exception as e:
        logger.error(f"Errore controllo encoder: {str(e)}")
        return False

async def split_video(input_path):
    logger.info(f"Starting video split for: {input_path}")

    # Costanti per la gestione avanzata degli ultimi segmenti
    LAST_SEGMENT_TOLERANCE = 30  # ±30 secondi di tolleranza
    MIN_LAST_SEGMENT_DURATION = 30  # Durata minima per l'ultimo segmento
    MERGE_LAST_SEGMENTS = True  # Abilita merge automatico finale

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file non trovato: {input_path}")
    
    original_size = os.path.getsize(input_path)
    if original_size == 0:
        raise ValueError("Il file di input è vuoto")

    split_config = config["SPLIT_SETTINGS"]
    MAX_FILE_SIZE = split_config["MAX_FILE_SIZE_MB"] * 1000**2
    MIN_FILE_SIZE = split_config["MIN_FILE_SIZE_MB"] * 1000**2
    MERGE_THRESHOLD = split_config["MERGE_THRESHOLD_MB"] * 1000**2
    MAX_RETRIES = split_config["MAX_RETRIES_SPLIT"]
    MID_TARGET_RATIO = split_config.get("MID_TARGET_RATIO", 0.75)
    DROP_SEGMENT_THRESHOLD = split_config.get("DROP_SEGMENT_THRESHOLD_SEC", 30.0)

    logger.info(
        f"Max file size: {MAX_FILE_SIZE/1000**2:.2f}MB\n"
        f"Min file size: {MIN_FILE_SIZE/1000**2:.2f}MB\n"
        f"Merge threshold: {MERGE_THRESHOLD/1000**2:.2f}MB\n"
        f"Target ratio: {MID_TARGET_RATIO}\n"
        f"Drop segment threshold: {DROP_SEGMENT_THRESHOLD}s\n"
        f"Last segment tolerance: ±{LAST_SEGMENT_TOLERANCE}s"
    )

    if original_size <= MAX_FILE_SIZE:
        logger.info("File già sotto la dimensione massima. Skipping split.")
        return [input_path]

    MID_FILE_SIZE = MIN_FILE_SIZE + MID_TARGET_RATIO * (MAX_FILE_SIZE - MIN_FILE_SIZE)
    logger.info(f"Intermediate target file size: {MID_FILE_SIZE/1000**2:.2f}MB")

    output_dir = os.path.dirname(input_path)
    base_name = os.path.basename(input_path).rsplit('.', 1)[0]
    output_pattern = os.path.join(output_dir, f"{base_name}_part%03d.mp4")

    metadata = await get_video_metadata(input_path)
    total_duration = metadata["duration"]
    avg_bitrate = metadata["bit_rate"]
    logger.info(f"Analisi video - Durata: {total_duration:.2f}s - Bitrate medio: {avg_bitrate/1000:.2f}kbps")

    segments = []
    current_start = 0.0
    part_number = 1
    fixed_bitrate = avg_bitrate  # Bitrate rimane fisso

    async def calculate_adjustment(actual_size):
        if actual_size <= 0:
            logger.error("Dimensione effettiva non valida per il calcolo dell'adjustment")
            return 1.0
        range_size = MAX_FILE_SIZE - MIN_FILE_SIZE
        mid_size = MID_FILE_SIZE

        if MIN_FILE_SIZE <= actual_size <= MAX_FILE_SIZE:
            return 1.0

        if actual_size > MAX_FILE_SIZE:
            safe_limit = MAX_FILE_SIZE * 0.99
            target = mid_size
        else:
            safe_limit = MIN_FILE_SIZE * 1.01
            target = mid_size

        distance = abs(actual_size - mid_size) / range_size
        distance = min(distance, 1.0)

        damping = 0.3 + (0.5 * distance)
        raw_adj = target / actual_size
        adjustment = 1 + damping * (raw_adj - 1)

        projected_size = actual_size * adjustment
        if projected_size > MAX_FILE_SIZE or projected_size < MIN_FILE_SIZE:
            adjustment = safe_limit / actual_size

        return adjustment

    async def split_segment(start, target_duration, attempt, is_last_segment=False):
        nonlocal part_number, total_duration

        out_file = output_pattern.replace("%03d", f"{part_number:03d}")
        bar_id = f"split_{part_number}"
        start_time = time.time()
        
        try:
            await progress_manager.create_bar(
                bar_id=bar_id,
                desc=f"Part {part_number:03d}",
                total=target_duration,
                unit='s'
            )

# Sostituisci la sezione del comando ffmpeg nello split_segment con:
            cmd = [
                "ffmpeg",
                "-y",
                "-threads", "0",
                "-ss", str(start),
                "-i", input_path,
                "-t", str(target_duration),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-profile:v", "main",
                "-tune", "zerolatency",
                "-x264-params", "nal-hrd=cbr",
                "-b:v", f"{int(fixed_bitrate//1000)}k",
                "-g", "60",
                "-bf", "1",
                "-c:a", "aac",
                "-b:a", "128k",
                "-f", "mp4",
                "-movflags", "+faststart",
                "-nostdin",
                "-progress", "pipe:1",
                out_file
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            progress_pattern = re.compile(r"out_time_us=(\d+)")
            
            async def read_progress():
                if proc.stdout is None:
                    logger.error("Impossibile leggere stdout: processo non avviato correttamente.")
                    return
                buffer = b""
                while True:
                    try:
                        chunk = await asyncio.wait_for(proc.stdout.read(4096), timeout=0.1)
                        if not chunk:
                            break
                            
                        buffer += chunk
                        lines = buffer.split(b'\n')
                        
                        for line in lines[:-1]:
                            line_str = line.decode().strip()
                            if '=' in line_str:
                                key, value = line_str.split("=", 1)
                                key = key.strip()
                                value = value.strip()

                                # Nuova gestione out_time_us senza regex
                                if key == "out_time_us":
                                    try:
                                        current_time = int(value) / 1_000_000
                                        await progress_manager.update_bar(bar_id, current_time)
                                    except ValueError:
                                        logger.debug(f"Ignorato valore non numerico: {value}")
                                        continue
                                
                        buffer = lines[-1] if lines else b""
                        
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"Errore lettura progresso: {str(e)}")
                        break
                            
                if buffer:
                    line_str = buffer.decode().strip()
                    match = progress_pattern.search(line_str)
                    if match:
                        current_time = int(match.group(1)) / 1_000_000
                        await progress_manager.update_bar(bar_id, current_time)

            progress_task = asyncio.create_task(read_progress())
            
            try:
                await asyncio.wait_for(proc.wait(), timeout=7200)
            except asyncio.TimeoutError:
                logger.error("Timeout durante lo split del segmento")
                proc.kill()
                raise
            finally:
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass

            if proc.returncode != 0:
                error_msg = (await proc.stderr.read()).decode()
                logger.error(f"Errore FFmpeg (code {proc.returncode}): {error_msg}")
                raise RuntimeError(f"Errore FFmpeg (code {proc.returncode})")

            out_file = await fix_metadata(out_file)
            await progress_manager.update_bar(bar_id, target_duration)
            
            metadata_seg = await get_video_metadata(out_file)
            actual_duration = metadata_seg["duration"]
            actual_size = metadata_seg["file_size"]

            logger.info(
                f"Segment {part_number:03d} - "
                f"Size: {actual_size/1e6:.2f}MB "
                f"(MAX: {MAX_FILE_SIZE/1e6:.2f}MB, MIN: {MIN_FILE_SIZE/1e6:.2f}MB) "
                f"Durata originale: {target_duration:.2f}s "
                f"Durata effettiva: {actual_duration:.2f}s"
            )

            # 1. Controllo avanzato residuo finale
            remaining_after_this = total_duration - (start + actual_duration)
            is_last_segment = (
                is_last_segment or 
                abs(remaining_after_this) <= LAST_SEGMENT_TOLERANCE or 
                remaining_after_this <= DROP_SEGMENT_THRESHOLD
            )

            # 2. Gestione ultimo segmento speciale
            if is_last_segment:
                if actual_size > MAX_FILE_SIZE:
                    adjustment = await calculate_adjustment(actual_size)
                    new_duration = target_duration * adjustment
                    logger.info(f"📉 Ultimo segmento sopra MAX: Riduzione del {(1-adjustment)*100:.1f}%")
                    os.remove(out_file)
                    return ("retry", new_duration, attempt + 1)
                
                if actual_duration < MIN_LAST_SEGMENT_DURATION:
                    logger.info(f"🔻 Ultimo segmento troppo corto: {actual_duration}s < {MIN_LAST_SEGMENT_DURATION}s")
                    os.remove(out_file)
                    return ("dropped", actual_duration)
                
                logger.info(f"🏁 Ultimo segmento accettato (Residuo: {remaining_after_this:.1f}s)")
                return ("success", out_file, actual_duration)

            # 3. Gestione generale dimensioni
            if actual_size > MAX_FILE_SIZE:
                adjustment = await calculate_adjustment(actual_size)
                new_duration = target_duration * adjustment
                logger.info(f"📉 Riduzione durata del {(1-adjustment)*100:.1f}%")
                os.remove(out_file)
                return ("retry", new_duration, attempt + 1)

            elif actual_size < MIN_FILE_SIZE:
                adjustment = await calculate_adjustment(actual_size)
                new_duration = min(target_duration * adjustment, total_duration - start)
                logger.info(f"📈 Espansione durata del {(adjustment-1)*100:.1f}%")
                os.remove(out_file)
                return ("retry", new_duration, attempt + 1)

            else:
                logger.info(f"✅ Segmento valido!")
                return ("success", out_file, actual_duration)

        except Exception as e:
            if os.path.exists(out_file):
                os.remove(out_file)
            raise
        finally:
            await progress_manager.close_bar(bar_id)
            logger.info(f"⏱ Tempo totale: {time.time() - start_time:.2f}s")

    while current_start < total_duration - 1:
        remaining_duration = total_duration - current_start
        
        # 1. Controllo tolleranza iniziale
        if abs(remaining_duration) <= LAST_SEGMENT_TOLERANCE:
            logger.info(f"⏹️ Residuo entro tolleranza iniziale: {remaining_duration:.1f}s")
            break

        # 2. Controllo soglia scarto
        if remaining_duration <= DROP_SEGMENT_THRESHOLD:
            logger.info(f"⏹️ Residuo sotto soglia scarto: {remaining_duration:.1f}s ≤ {DROP_SEGMENT_THRESHOLD}s")
            break

        # Calcolo parametri segmento
        target_duration = (MID_FILE_SIZE * 8) / fixed_bitrate
        target_duration = min(target_duration, remaining_duration)
        remaining_after_split = remaining_duration - target_duration
        is_last_segment = (
            abs(remaining_after_split) <= LAST_SEGMENT_TOLERANCE or 
            remaining_after_split <= DROP_SEGMENT_THRESHOLD
        )

        attempt = 1
        while attempt <= MAX_RETRIES:
            logger.info(f"Tentativo {attempt}/{MAX_RETRIES} per parte {part_number:03d}")
            try:
                result = await split_segment(current_start, target_duration, attempt, is_last_segment)
            except asyncio.TimeoutError:
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(f"Timeout dopo {MAX_RETRIES} tentativi")
                logger.warning("Timeout, riprovo con durata ridotta...")
                target_duration *= 0.8
                attempt += 1
                continue
            except Exception as e:
                logger.error(f"Errore durante lo split: {str(e)}")
                if attempt >= MAX_RETRIES:
                    raise
                attempt += 1
                continue

            if result[0] == "success":
                _, segment_path, seg_duration = result
                segments.append(segment_path)
                current_start += seg_duration
                part_number += 1
                break
            elif result[0] == "dropped":
                _, seg_duration = result
                current_start += seg_duration
                part_number += 1
                break
            elif result[0] == "retry":
                _, new_target_duration, attempt = result
                target_duration = new_target_duration
                logger.info(f"New target duration: {target_duration:.2f}s (Attempt {attempt})")
        else:
            raise RuntimeError(f"Stato sconosciuto: {result[0]}")

    # Merge finale degli ultimi segmenti se necessario
    if MERGE_LAST_SEGMENTS and len(segments) > 1:
        last_seg = segments[-1]
        last_size = os.path.getsize(last_seg)
        penultimate_seg = segments[-2]
        penultimate_size = os.path.getsize(penultimate_seg)

        if last_size < MIN_FILE_SIZE and (penultimate_size + last_size) <= MAX_FILE_SIZE:
            logger.info("🔀 Merge automatico degli ultimi due segmenti")
            try:
                merged = await merge_segments([penultimate_seg, last_seg])
                segments = segments[:-2] + [merged]
            except Exception as e:
                logger.error(f"Merge fallito: {str(e)}")

    final_segments = await optimize_segments(segments, MAX_FILE_SIZE, MERGE_THRESHOLD, MIN_FILE_SIZE)
    
    # Validazione finale
    for seg in final_segments:
        seg_size = os.path.getsize(seg)
        if seg_size > MAX_FILE_SIZE:
            logger.error(f"ERRORE: Segmento {os.path.basename(seg)} supera MAX_SIZE ({seg_size/1e6:.2f}MB)")
            raise ValueError("Dimensione segmento non conforme dopo l'ottimizzazione")
    
    logger.info(f"Splitting completato con {len(final_segments)} segmenti validi")
    return final_segments


# -------------------------------------------------------------------------------
# FUNZIONE: upload_segment
# -------------------------------------------------------------------------------

async def upload_segment(segment, vod, part_number, total_parts):
    bar_id = f"upload_{part_number}"
    thumbnail_path = None
    try:
        # --- Controllo del moov atom e correzione metadata ---
        async def check_moov_position(file_path):
            """Verifica la presenza del flag faststart nei metadati"""
            cmd = [
                "ffprobe",
                "-loglevel", "info",
                "-show_entries", "format_tags=faststart",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            stdout, _ = await proc.communicate()
            return "faststart" in stdout.decode().strip()

        if not await check_moov_position(segment):
            logger.warning("File non ottimizzato per streaming, applico correzione...")
            try:
                original_size = os.path.getsize(segment)
                segment = await fix_metadata(segment)
                new_size = os.path.getsize(segment)
                logger.info(f"Metadata fissati | Originale: {original_size/1e6:.2f}MB -> Nuovo: {new_size/1e6:.2f}MB")
            except Exception as e:
                logger.error(f"Correzione metadata fallita: {str(e)}")
                raise

        # --- Validazione metadati ---
        metadata = await get_video_metadata(segment)
        required_metadata = ['duration', 'width', 'height', 'bit_rate']
        if not all(key in metadata for key in required_metadata):
            raise ValueError("Metadati video incompleti")
            
        if metadata['duration'] < 1:
            raise ValueError(f"Durata video non valida: {metadata['duration']}s")

        # --- Generazione della thumbnail ---
        thumbnail_path = await generate_thumbnail(segment)
        if not os.path.exists(thumbnail_path):
            raise FileNotFoundError("Thumbnail non generata correttamente")

        # --- Configurazione della connessione ---
        channel_id = int(config["TELEGRAM_CHANNEL_ID"])
        try:
            created_at = datetime.fromisoformat(vod['created_at'].replace('Z', ''))
            formatted_date = created_at.strftime("%d/%m/%Y")
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Errore formattazione data: {str(e)}")
            formatted_date = "Data non disponibile"
        
        caption = f"{vod['title']} ({formatted_date}) - Part {part_number}/{total_parts}"
        logger.info(f"Caption per Telegram: {caption}")

        # Connessione robusta con backoff
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                if not client.is_connected():
                    await client.connect()
                    await asyncio.sleep(2 ** attempt)  # Backoff esponenziale
                await client.get_me()
                channel = await client.get_entity(channel_id)
                break
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise RuntimeError(f"Connessione fallita dopo {max_attempts} tentativi: {str(e)}")
                logger.warning(f"Tentativo connessione {attempt+1}/{max_attempts} fallito, riprovo...")

        # --- Upload con gestione avanzata del progresso ---
        file_size = os.path.getsize(segment)
        await progress_manager.create_bar(bar_id, f"Upload {part_number}/{total_parts}", file_size, 'B')
        last_progress_update = 0
        def progress_callback(uploaded_bytes, total_bytes):
            nonlocal last_progress_update
            now = time.time()
            if now - last_progress_update > 0.3:  # Aggiorna ogni 300ms
                asyncio.create_task(progress_manager.update_bar(bar_id, uploaded_bytes))
                last_progress_update = now

        # --- Upload file rispettando il limite fisso del chunk size ---
        # Telegram impone che ogni parte sia di 512 KB (l'ultima parte può essere più piccola)
        chunk_size = 512  # in KB (fisso)
        file = await client.upload_file(
            segment,
            progress_callback=progress_callback,
            part_size_kb=chunk_size,
            file_size=file_size
        )

        # --- Invio del file su Telegram ---
        await client.send_file(
            channel,
            file,
            caption=caption[:1024],  # Limita la caption a 1024 caratteri
            thumb=thumbnail_path,
            attributes=[
                DocumentAttributeVideo(
                    duration=int(metadata['duration']),
                    w=metadata['width'],
                    h=metadata['height'],
                    supports_streaming=True  # Abilita lo streaming
                )
            ],
            mime_type='video/mp4',
            parse_mode='html',
            part_count=total_parts,
            force_document=False,  # Importante per lo streaming
            allow_cache=False,
            background=False
        )

        await progress_manager.close_bar(bar_id)
        logger.info(f"Parte {part_number}/{total_parts} caricata correttamente")
        
    except Exception as e:
        await progress_manager.close_bar(bar_id)
        logger.error(f"Errore upload parte {part_number}: {str(e)}", exc_info=True)
        await log_operation(vod['id'], 'upload', 'failed', f"Part {part_number}: {str(e)}")
        raise
    finally:
        # Pulizia affidabile della thumbnail
        if thumbnail_path and os.path.exists(thumbnail_path):
            try:
                os.remove(thumbnail_path)
                logger.debug(f"Thumbnail pulita: {os.path.basename(thumbnail_path)}")
            except Exception as e:
                logger.warning(f"Errore pulizia thumbnail: {str(e)}")

async def main_loop():
    await init_db()
    await check_dependencies()
    logger.info("Connessione a Telegram...")
    try:
        await client.start(config["TELEGRAM_PHONE_NUMBER"])
    except Exception as e:
        logger.critical(f"Connessione Telegram fallita: {str(e)}")
        await shutdown_sequence()
        return
    logger.info("Servizio inizializzato")
    try:
        while True:
            try:
                await config.reload()
                await log_vod_stats()
                
                if config["VOD_SETTINGS"].get("AUTO_CHECK_VODS", True):
                    await check_new_vods()
                
                pending_vods = await get_pending_vods()
                
                for vod in pending_vods:
                    vod_id = vod['id']
                    vod_dir = os.path.join(TEMP_ROOT, vod_id)
                    os.makedirs(vod_dir, exist_ok=True)
                    
                    await update_vod_status(vod_id, VodStatus.PROCESSING.value, increment_retries=False)
                    try:
                        await log_operation(vod_id, 'process', 'started')
                        file_path = await download_vod(vod, token_manager)
                        segments = await split_video(file_path)
                        total_parts = len(segments)
                        
                        for idx, segment in enumerate(segments):
                            await upload_segment(segment, vod, idx+1, total_parts)
                            await asyncio.sleep(config["SPLIT_SETTINGS"].get("UPLOAD_DELAY", 5))
                        
                        await update_vod_status(vod_id, VodStatus.COMPLETED.value)
                        await log_operation(vod_id, 'upload', 'completed', f"{total_parts} parts")
                    
                    except Exception as e:
                        await handle_vod_error(vod, e)
                    
                    finally:
                        await cleanup_vod(vod_id, vod_dir)
                
                await asyncio.sleep(config["SPLIT_SETTINGS"].get("PROCESSING_INTERVAL", 1800))
            
            except asyncio.CancelledError:
                logger.info("Shutdown request ricevuto")
                break
            
            except Exception as e:
                logger.error(f"Errore critico nel main loop: {str(e)}", exc_info=True)
                await asyncio.sleep(10)
    
    except KeyboardInterrupt:
        logger.info("Interruzione da tastiera ricevuta")
    
    finally:
        await shutdown_sequence()

async def shutdown_sequence():
    try:
        await progress_manager.close_all()
        logger.info("Disconnessione da Telegram...")
        await client.disconnect()
        logger.info("Disconnessione riuscita")
    except Exception as e:
        logger.error(f"Errore disconnessione: {str(e)}")
    finally:
        logger.info("Servizio fermato")

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        asyncio.run(shutdown_sequence())
