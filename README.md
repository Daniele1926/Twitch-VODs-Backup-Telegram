# Twitch-VODs-Backup-Telegram
A complete python bot for continuos backup from Twitch to Telegram channel.
This code use YT_DLP downloader, SQLite for DB and FFmpeg and FFprobe for merging/split/metadata stuff
This code can also download sub-only VODs by providing your Twitch cookies in cookies.txt (netscape format)

FEATURES:

- Automatic fetch new vods and tracking live vods
- Automatic download new VODs
- Support for only subs VODs
- SQL Database to track failed VODs, completed VODs, currently in live VODs and retries
- Smart automatic split for VODs larger than 2/4GB (depends on Telegram limits)
- Telegram caption with thumbnail, streaming support, VOD title, date and part

INSTALL

1. Download repository
2. use pip install  ffmpeg ffprobe  aiohttp aiofiles cryptg aiosqlite yt_dlp shutil psutil telethon tqdm (if you are root user, you need to write --break-system-packages at the end)
3. modify config.json according to the little guide inside (if you have telegram premium you can do 4GB instead of 2GB upload size
4. if you want to download sub-only vods, you need user auth token and cookies from twitch in netscape format (you can with cookie editor extension)



*RUN*

WINDOWS: 

- Download FFmpeg and FFprobe .exe, put in the folder and just double click start.bat


LINUX: 

- Go to directory and launch by python3 __ main __.py



DISCLAIMER: 
- This tool is 99% made by DeepSeek and ChatGPT. I'm not a developer.
