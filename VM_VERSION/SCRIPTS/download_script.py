import yt_dlp
import sys
import os

FFMPEG_PATH = "/home/osbdet/ffmpeg"
DOWNLOAD_PATH = "/tmp/nifi_download/"
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

def download_song(song_name):
    ydl_opts = {
        'format': 'bestaudio/best',
        'default_search': 'ytsearch1',
        'noplaylist': True,
        'outtmpl': f'{DOWNLOAD_PATH}/%(title)s.%(ext)s',
        'ffmpeg_location': FFMPEG_PATH,
    }

    # Pre-download info
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(song_name, download=False)
        filename = ydl.prepare_filename(info_dict)

        if os.path.exists(filename):
            print(f"SKIPPING: Already exists: {filename}")
        else:
            ydl.download([song_name])
            print(f"Downloaded: {filename}")

if __name__ == "__main__":
    try:
        # Read plain text input from STDIN (just the song name)
        song = sys.stdin.read().strip()
        if song:
            print(f"Downloading: {song}")
            download_song(song)
            print(f"Downloaded: {song}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)