# Modern-Data-Architectures

## PREREQUISITE
Every time you run this project in the VM, after closing the VM and shutting down all services, it seems to break hadoop and it will no longer start. I have a theory that it might be due to the external packages installed in order to do the audio conversion but I have yet to solve this.

## Data Ingestion Pipeline (NiFi + Python)

### Overview:
This stage is responsible for ingesting audio content from **YouTube**, converting it into `.webm` format, and saving it into **HDFS (Bronze Layer)** using **Apache NiFi** and custom **Python scripts**.

### Tools Used:
- **Apache NiFi** – Orchestrates the entire flow with processors.
- **yt-dlp (YouTube Downloader)** – Fetches audio streams from YouTube.
- **Hadoop HDFS** – Destination storage layer (`/lakehouse/bronze/webm/`).

### Flow Architecture:

#### LEFT SIDE: Trigger & Download
| Processor              | Description                                                                 |
|------------------------|------------------------------------------------------------------------------|
| `GetFile`              | Loads a `song_list.txt` file with one song query per line                   |
| `SplitText`            | Splits list into individual song queries (1 per FlowFile)                   |
| `ExecuteStreamCommand` | Calls a Python script to download `.webm` files via **yt-dlp**              |
| Output Directory       | Files are saved temporarily in `/tmp/nifi_download/` on local disk          |

#### RIGHT SIDE: Transfer to HDFS & Cleanup
| Processor              | Description                                                                 |
|------------------------|------------------------------------------------------------------------------|
| `ListFile`             | Monitors `/tmp/nifi_download/` for new `.webm` files                        |
| `FetchFile`            | Reads the files from local disk                                             |
| `UpdateAttribute`      | Applies filename normalization before writing to HDFS                       |
| `PutHDFS`              | Uploads each `.webm` to `hdfs://localhost:9000/lakehouse/bronze/webm/`      |
| `ExecuteStreamCommand` (Cleanup) | Deletes `.webm` files from local disk after HDFS upload        |

### Directory Structure:
```
/lakehouse/
├── bronze/
│   └── webm/              ← Raw audio files (.webm) downloaded from YouTube
└── silver/
    ├── mp3_binary/        ← Processed audio files in binary format (.mp3)
    └── logs/              ← Conversion logs (Parquet)
```

### Filename Normalization (UpdateAttribute):
To avoid special character issues (e.g., spaces, quotes, accents), NiFi uses an **`UpdateAttribute`** processor (placed **between `FetchFile` → `PutHDFS`**) with a custom **filename sanitization strategy**, such as:

```regex
[^A-Za-z0-9._-]
```

This replaces invalid characters with underscores to ensure compatibility across HDFS and downstream processing.

You can configure the `UpdateAttribute` processor with:
- **`filename` attribute override**:
```text
${filename:replaceAll('[^A-Za-z0-9._-]', '_')}
```

### Known Issues & Workarounds:
| Issue | Resolution |
|-------|------------|
| `PutHDFS` uploads song_list.txt | Add a RouteOnAttribute or move `.txt` handling separately |
| FlowFile loops continuously | Use `DeleteFile` after `GetFile`, or remove input file manually |
| Cleanup script doesn't delete | Ensure `filename` and `absolute.path` are passed as environment variables |
| Right-side processors don’t trigger | Temporary workaround: toggle ListFile's regex pattern (`.*\.webm → .*\.webm' → back`) to reset NiFi internal file state |

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

### Shareable NiFi Template
Template Goes Here

---

## Notebook 2 – Audio Conversion to Binary & Storage in HDFS (Silver Layer)

### Overview:
This stage handles the transformation of `.webm` audio files (Bronze Layer) into `.mp3` format, converts them into binary content, and stores the results in **HDFS Silver Layer** in **Parquet format** for efficient downstream processing.

### Input & Output Paths:
- **Input**: `hdfs://localhost:9000/lakehouse/bronze/webm/`
- **Output (Binary)**: `hdfs://localhost:9000/lakehouse/silver/mp3_binary/`
- **Logs (Optional)**: `hdfs://localhost:9000/lakehouse/silver/logs/audio_conversion_logs/`

### Core Logic Flow:
1. Spark lists all `.webm` files directly from HDFS using Hadoop’s FileSystem API.
2. Files are loaded **in parallel** via RDD and enriched with metadata (filename, size, etc.).
3. Each file is **copied locally** from HDFS → `/tmp/webm_conversion/input/`.
4. `ffmpeg` is used to convert `.webm` → `.mp3` (audio-only).
5. The resulting `.mp3` is read in **binary format** (`read().encode()`).
6. Spark writes binary audio + metadata as **Parquet files to Silver layer**.
7. A separate Parquet-based **log table** tracks conversion status, errors, and timestamps.

### Key Features:
- Efficient parallelization using Spark RDDs.
- Flexible, metadata-rich processing pipeline.
- Spark-compatible binary storage via Parquet.
- Logs and errors are traceable per file (including FFmpeg stderr output).
- Optional schema-checking and directory guards (to handle Spark/HDFS edge cases).

### Notes:
- Conversion failures often stem from:
  - Special characters in filenames
  - File already exists (`ffmpeg` overwrite prompt)
  - URISyntax errors (resolved via filename normalization from NiFi)
- Directory cleanup between runs is advised to avoid residual files.
- Reading the binary parquet in a Spark DataFrame shows binary audio content as `bytearray(...)`.

### Sample Schema of Binary DataFrame:
| filename                          | size     | content (binary)   | timestamp | status  | message            |
|----------------------------------|----------|--------------------|-----------|---------|---------------------|
| Ed_Sheeran_Shape_Of_You.mp3      | 4171750  | bytearray(...)     | ...       | Success | Converted & Stored  |

### Log Sample:
| filename         | status | message                            | timestamp |
|------------------|--------|------------------------------------|-----------|
| songname.mp3     | Failed | File exists / Conversion Error     | ...       |

---

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