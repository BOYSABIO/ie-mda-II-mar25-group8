import os
import subprocess

WEBM_DIR = "/tmp"
MP3_DIR = "/tmp"

for file in os.listdir(WEBM_DIR):
    if file.endswith(".webm"):
        input_path = os.path.join(WEBM_DIR, file)
        output_path = os.path.join(MP3_DIR, file.replace(".webm", ".mp3"))

        print(f"Converting {input_path} to {output_path}")
        cmd = [
            "ffmpeg",
            "-i", input_path,
            "-vn",
            "-ab", "192k",
            "-ar", "44100",
            "-y",
            output_path
        ]

        try:
            subprocess.run(cmd, check=True)
            print(f"Converted: {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error converting {input_path}: {e}")
