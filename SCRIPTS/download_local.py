import yt_dlp
import os
import numpy as np
import librosa
import shutil

# Set up paths
songs_txt_path = "song_list.txt"
download_path = "songs_mp3"
fingerprint_output_path = "fingerprints"

os.makedirs(download_path, exist_ok=True)
os.makedirs(fingerprint_output_path, exist_ok=True)

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
    """Generate fingerprint from MP3 and save .npy file"""
    try:
        y, sr = librosa.load(file_path, sr=44100)
        D = librosa.amplitude_to_db(np.abs(librosa.stft(y)), ref=np.max)
        peaks = np.argwhere(D > np.percentile(D, 95))  # Top 5% peaks
        fingerprint = peaks.tolist()
        base_name = os.path.basename(file_path).replace(".mp3", "")
        out_path = os.path.join(fingerprint_output_path, f"{base_name}.npy")
        np.save(out_path, fingerprint)
        print(f"[✓] Fingerprint saved: {base_name}.npy")
        
        # Delete original .mp3 after conversion
        os.remove(file_path)
        print(f"[🗑️] Removed MP3: {file_path}")
    except Exception as e:
        print(f"[!] Error processing {file_path}: {e}")

# Load song list from songs_list.txt
if os.path.exists(songs_txt_path):
    with open(songs_txt_path, "r") as f:
        songs = [line.strip() for line in f.readlines() if line.strip()]
else:
    print("[!] No songs_list.txt found.")
    songs = []

# Process each song
for song in songs:
    download_song(song)

# Process downloaded MP3 files
for file in os.listdir(download_path):
    if file.endswith(".mp3"):
        file_path = os.path.join(download_path, file)
        generate_fingerprint(file_path)

print("\n[✓] All songs processed and fingerprints saved.")
