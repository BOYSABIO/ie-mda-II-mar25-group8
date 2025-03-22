## VIRTUAL MACHINE ENVIRONMENT BEFORE SWITCHING TO DATABRICKS

### PREREQUISITE
Every time you run this project in the VM, after closing the VM and shutting down all services, it seems to break hadoop and it will no longer start. I have a theory that it might be due to the external packages installed in order to do the audio conversion but I have yet to solve this.

### Current Progress Summary

- Audio conversion from `.webm` to `.mp3` and binary storage in **HDFS Silver Layer** completed via Spark.
- Conversion logs saved in Parquet under `lakehouse/silver/logs/audio_conversion_logs/`.
- Local fingerprint generation now working successfully using `librosa`.
- `.npy` fingerprint files can be converted into `.parquet` and optionally pushed to HDFS manually.

### Known Issues & Workarounds

#### **NiFi File Detection Delay**
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

> _Might be caused by NiFi's internal file tracker cache not refreshing properly when new files are created during runtime._

#### **Writing `.npy`-based Fingerprints to HDFS via Spark**
**Issue:** Writing large `.npy` fingerprint files as Parquet into HDFS via Spark results in out-of-memory errors or failed task stage due to high task size.

**Workaround:**
- Write `.parquet` files **locally** first using:
  ```python
  fingerprint_df.write.mode("overwrite").parquet("parquet/")
  ```
- Then manually upload into HDFS using:
  ```bash
  hdfs dfs -copyFromLocal parquet/ /warehouse/fingerprints/
  ```

---

### Python Virtual Environment Setup

Since some Python libraries like `librosa`, `yt-dlp`, `ffmpeg-python`, `matplotlib`, and `numpy` are not included in default Spark environments or may conflict with other system packages, it's highly recommended to use a **dedicated virtual environment** when working outside Spark (e.g., for downloading or fingerprinting).

#### Steps:
```bash
python3 -m venv venv_audio
source venv_audio/bin/activate

# Install dependencies
pip install yt-dlp librosa ffmpeg-python numpy matplotlib
```

Use this environment when running local Python scripts such as:
- `song_downloader_and_fingerprinter.py`

> Note: Spark itself won’t use this venv unless explicitly configured. This is intended for **preprocessing scripts outside Spark**.

---

### Next Steps

- Limit audio download duration via YouTube API to ~30 seconds for smaller file sizes (reduce load on fingerprinting stage).
- Finalize Shazam-style matching notebook:
  - Load a short MP3 snippet
  - Extract fingerprint
  - Match against stored fingerprint database (using cosine similarity / hashing).
- Store matching results in separate Parquet logs if needed.

### Paths Summary
- Fingerprints (.npy): `fingerprints/`
- Converted Parquet Fingerprints (local): `parquet/`
- HDFS (if manually copied): `hdfs://localhost:9000/warehouse/fingerprints/`

---

_This update summarizes our current stable implementation path and outlines how to continue development and optimization from here._

---

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