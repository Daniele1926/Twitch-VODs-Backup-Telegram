# Twitch-VODs-Backup-Telegram
A complete python bot for continuos backup from Twitch to Telegram channel.
This code can also download sub-only VODs by providing your Twitch cookies in cookies.txt (netscape format)

INSTALL

1. Download repository
2. use pip install  ffmpeg ffprobe  aiohttp aiofiles cryptg aiosqlite yt_dlp shutil psutil telethon tqdm (if you are root user, you need to write --break-system-packages at the end)
3. modify config.json according to the little guide inside (if you have telegram premium you can do 4GB instead of 2GB upload size
4. if you want to download sub-only vods, you need user auth token and cookies from twitch in netscape format (you can with cookie editor extension)

RUN

ON WINDOWS
Download FFmpeg and FFprobe .exe, put in the folder and just double click start.bat

ON LINUX
Go to directory and launch with "python3 __main__.py"



DISCLAIMER
This tool is 99% made by DeepSeek and ChatGPT. I'm not a developer.
