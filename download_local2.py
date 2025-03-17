import yt_dlp
import os
import numpy as np
import librosa
import json
import shutil

# Set up paths
songs_txt_path = "song_list.txt"
download_path = "songs_mp3"
fingerprint_output_path = "fingerprints"
flattened_output_path = "flattened"

os.makedirs(download_path, exist_ok=True)
os.makedirs(fingerprint_output_path, exist_ok=True)
os.makedirs(flattened_output_path, exist_ok=True)

def download_song(song_name):
    """Download a song from YouTube and convert to MP3"""
    ydl_opts = {
        'format': 'bestaudio/best',
        'default_search': 'ytsearch1',
        'noplaylist': True,
        'outtmpl': f'{download_path}/%(title)s.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'quiet': False
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([song_name])
        print(f"[✓] Downloaded: {song_name}")
    except Exception as e:
        print(f"[!] Error downloading {song_name}: {e}")

def generate_fingerprint(file_path):
    """Generate fingerprint from MP3, save .npy and flattened .json"""
    try:
        y, sr = librosa.load(file_path, sr=44100)
        D = librosa.amplitude_to_db(np.abs(librosa.stft(y)), ref=np.max)
        peaks = np.argwhere(D > np.percentile(D, 95))  # Top 5% peaks

        # Save .npy
        base_name = os.path.basename(file_path).replace(".mp3", "")
        npy_path = os.path.join(fingerprint_output_path, f"{base_name}.npy")
        np.save(npy_path, peaks)
        print(f"[✓] Fingerprint saved: {npy_path}")

        # Save flattened JSON
        flattened = peaks.tolist()
        json_path = os.path.join(flattened_output_path, f"{base_name}.json")
        with open(json_path, "w") as jf:
            json.dump(flattened, jf)
        print(f"[✓] Flattened fingerprint saved: {json_path}")

        # Cleanup original MP3
        os.remove(file_path)
        print(f"[🗑️] Removed MP3: {file_path}")

    except Exception as e:
        print(f"[!] Error processing {file_path}: {e}")

# Load song list
if os.path.exists(songs_txt_path):
    with open(songs_txt_path, "r") as f:
        songs = [line.strip() for line in f if line.strip()]
else:
    print("[!] No song_list.txt found.")
    songs = []

# Download all songs
for song in songs:
    download_song(song)

# Process MP3s
for file in os.listdir(download_path):
    if file.endswith(".mp3"):
        generate_fingerprint(os.path.join(download_path, file))

print("\n[✓] All songs processed. JSON features are ready for HDFS push.")