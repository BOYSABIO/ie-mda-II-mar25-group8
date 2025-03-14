# Modern-Data-Architectures

## SUMMARY (IN PROGRESS)
This project replicates the core logic of the Shazam app in a Big Data architecture using:

- **Apache NiFi** – for ingestion, flow control and automation  
- **Apache Kafka (planned)** – for distributed streaming (optional layer)  
- **Apache Spark (next stage)** – for MP3 conversion and fingerprinting  
- **Hadoop (HDFS)** – for storage
- **yt-dlp + Python** – for gathering audio through API

---

## NIFI Architecture Flow

### LEFT SIDE – Trigger & Song Downloader
- `GetFile` – reads a song list `song_list.txt` file (You need to create it)  
- `SplitText` – splits list into individual song search queries  
- `ExecuteStreamCommand` – runs a Python script that uses `yt_dlp` to download `.webm` audio files into `/tmp/nifi_download`  

### RIGHT SIDE – HDFS Uploader & Cleanup
- `ListFile` – detects `.webm` files in the temp download directory  
- `FetchFile` – reads actual files from disk  
- `PutHDFS` – stores files into Hadoop `/lakehouse/bronze/webm`  
- `ExecuteStreamCommand` (Cleanup) – deletes each `.webm` file after HDFS write to conserve disk space  

---

## Known Edge Case / Workaround

**Issue:** After running the left-side processors, the right-side ingestion pipeline (`ListFile → FetchFile → PutHDFS`) sometimes does not trigger.

**Workaround:**
1. Stop all processors
2. In `ListFile`, temporarily change **File Filter** from:
   ```
   .*\.webm
   ```
   → to:
   ```
   .*\.webm'
   ```
3. Run processors once
4. Stop processors again
5. Revert File Filter back to:
   ```
   .*\.webm
   ```
6. Run all processors again – everything resumes normally

> *Might be caused by NiFi's internal file tracker cache not refreshing properly when new files are created during runtime. This workaround forces the cache to update and resume detection.*

---

## Next Steps (Mostly Finished Locally)
- Add Spark job to convert `.webm → .mp3` (`/lakehouse/silver/audio_mp3`)
- Apply Spark fingerprinting & song matching (already prototyped in notebook)
- Add Kafka layer for real-time song ingestion at scale (Optional)

## **GitHub Setup Guide**

### (1) Generate an SSH Key
1. Open the **Terminal**.
2. Run the following command:
   ```sh
   ssh-keygen -t rsa -b 4096 -C "YOUR@EMAIL.com"
   ```
3. Follow the prompts:
   - **File location:** Press **Enter** (use the default).
   - **Overwrite existing key?** Type **y** if asked.
   - **Passphrase:** Press **Enter** (leave blank).
   - A **fingerprint** will be displayed, meaning it's done.

---

### (2) Add the SSH Key to GitHub
1. Run this command to display your SSH key:
   ```sh
   cat ~/.ssh/id_rsa.pub
   ```
2. **Copy the entire key** that appears in the terminal.
3. **Add it to GitHub:**
   - Go to **GitHub**.
   - Click on your **profile picture (top right)** → **Settings**.
   - Navigate to **SSH and GPG Keys** → **New SSH Key**.
   - Give it a **name**, **paste the key**, and **Save**.

---

### (3) Test the SSH Connection
Run:
```sh
ssh -T git@github.com
```
- If successful, you should see:
  ```
  Hi <your-username>! You've successfully authenticated, but GitHub does not provide shell access.
  ```
- If you see an error, **retry** the setup and ensure you copied everything correctly.

---

### (4) Set the Remote Repository
To ensure you are connected to the correct repository, run:
```sh
git remote set-url origin git@github.com:BOYSABIO/Modern-Data-Architectures.git
```

---

### (5) Install Required Package (Ubuntu/Debian)
For our project, install **ffmpeg** by running:
```sh
sudo apt update && sudo apt install ffmpeg -y
```
This ensures that all necessary dependencies are installed.

---

### All Set!
You are now connected to GitHub and ready to work on the project.