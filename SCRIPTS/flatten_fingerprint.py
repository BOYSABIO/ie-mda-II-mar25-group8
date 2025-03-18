import yt_dlp
import librosa
import soundfile as sf
import numpy as np
import json
import os
import sys

DOWNLOAD_PATH = "/tmp/nifi_download/"
FEATURE_PATH = "/tmp/nifi_features/"
os.makedirs(DOWNLOAD_PATH, exist_ok=True)
os.makedirs(FEATURE_PATH, exist_ok=True)

def download_mp3(song_name):
    ydl_opts = {
        'format': 'bestaudio/best',
        'default_search': 'ytsearch1',
        'noplaylist': True,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': f'{DOWNLOAD_PATH}/%(title)s.%(ext)s'
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(song_name, download=False)
        filename = ydl.prepare_filename(info).replace(".webm", ".mp3")
        if not os.path.exists(filename):
            ydl.download([song_name])
    return filename

def extract_librosa_features(audio_path):
    try:
        y, sr = librosa.load(audio_path, sr=22050)

        # Extract features
        mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        zcr = librosa.feature.zero_crossing_rate(y)
        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)

        # Flatten for storage
        features = {
            "filename": os.path.basename(audio_path),
            "mfcc": mfcc.mean(axis=1).tolist(),
            "chroma": chroma.mean(axis=1).tolist(),
            "zero_crossing_rate": zcr.mean().item(),
            "tempo": tempo.item()
        }

        # Save
        base = os.path.splitext(os.path.basename(audio_path))[0]
        output_path = os.path.join(FEATURE_PATH, f"{base}_features.json")
        with open(output_path, 'w') as f:
            json.dump(features, f)
        print(f"✅ Features saved: {output_path}")
        return output_path

    except Exception as e:
        print(f"❌ Error processing {audio_path}: {e}")
        return None

if __name__ == "__main__":
    try:
        song = sys.stdin.read().strip()
        if song:
            mp3 = download_mp3(song)
            extract_librosa_features(mp3)
    except Exception as e:
        print(f"Script failed: {e}")
