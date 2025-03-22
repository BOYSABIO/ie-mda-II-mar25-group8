import yt_dlp
import os
import librosa
import librosa.display
import numpy as np
import matplotlib.pyplot as plt
import subprocess

FFMPEG_PATH = "/home/osbdet/ffmpeg"  # Adjust if needed

# Output folders
DOWNLOAD_DIR = "clip_download"
CLIP_DIR = "clip_mp3"
FINGERPRINT_DIR = "clip_fingerprint"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CLIP_DIR, exist_ok=True)
os.makedirs(FINGERPRINT_DIR, exist_ok=True)

def download_youtube_audio(song_name):
    """Download audio from YouTube and save as MP3"""
    ydl_opts = {
        'format': 'bestaudio/best',
        'default_search': 'ytsearch1',
        'noplaylist': True,
        'ffmpeg_location': FFMPEG_PATH,
        'outtmpl': f'{DOWNLOAD_DIR}/%(title)s.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(song_name, download=False)
        filename = ydl.prepare_filename(info).replace(".webm", ".mp3")
        if not os.path.exists(filename):
            ydl.download([song_name])
        return filename

def extract_clip(input_mp3, start_sec=0, duration_sec=10):
    """Trim the MP3 file to the first X seconds"""
    output_mp3 = os.path.join(CLIP_DIR, os.path.basename(input_mp3).replace(".mp3", "_clip.mp3"))
    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i", input_mp3,
        "-ss", str(start_sec),
        "-t", str(duration_sec),
        "-acodec", "copy",
        output_mp3
    ]
    subprocess.run(cmd, check=True)
    return output_mp3

def generate_fingerprint(file_path):
    """Create spectrogram fingerprint from audio file"""
    try:
        y, sr = librosa.load(file_path, sr=44100)
        D = librosa.amplitude_to_db(np.abs(librosa.stft(y)), ref=np.max)
        peaks = np.argwhere(D > np.percentile(D, 95))
        fingerprint = peaks.tolist()
        
        # Save fingerprint
        out_path = os.path.join(FINGERPRINT_DIR, os.path.basename(file_path).replace(".mp3", ".npy"))
        np.save(out_path, fingerprint)
        print(f"Fingerprint saved: {out_path}")

        # Optional: Save spectrogram image
        plt.figure(figsize=(10, 5))
        librosa.display.specshow(D, sr=sr, x_axis="time", y_axis="log")
        plt.title(f"Spectrogram: {os.path.basename(file_path)}")
        plt.colorbar()
        plt.tight_layout()
        plt.savefig(out_path.replace(".npy", ".png"))
        plt.close()

    except Exception as e:
        print(f"Error generating fingerprint for {file_path}: {e}")


def create_clip_fingerprint(song_name):
    print(f"\nProcessing song: {song_name}")
    mp3_path = download_youtube_audio(song_name)
    print(f"Downloaded MP3: {mp3_path}")
    
    clip_path = extract_clip(mp3_path, start_sec=0, duration_sec=15)
    print(f"Extracted clip: {clip_path}")
    
    generate_fingerprint(clip_path)


if __name__ == "__main__":
    song_query = input("Enter song name to fingerprint a clip: ")
    create_clip_fingerprint(song_query)
