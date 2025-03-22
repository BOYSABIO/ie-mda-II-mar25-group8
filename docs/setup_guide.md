## Setup Guide

Before running the Zasham audio matching system, please follow the steps below to install the required tools and prepare your environment.

### 1. Local Environment Setup

Install the following tools and libraries:

#### Required Tools:
- [FFmpeg](https://ffmpeg.org/download.html)  
  Required for audio conversion when using `yt-dlp`.  
  Make sure `ffmpeg` is added to your system's PATH after installation.

#### Python Libraries:
Install the required packages using pip:
```bash
pip install librosa yt-dlp pandas numpy
```

---

### 2. Run `AUDIO_PROCESSING.ipynb` (Locally)

This is the main notebook for local audio processing. It performs the following:
- Downloads full songs using `yt-dlp`
- Creates short random audio clips (10–20 seconds)
- Fingerprints both full songs and clips using `librosa`
- Flattens and exports all fingerprints into a CSV file:  
  `Flattened_Fingerprints.csv`

---

### 3. Upload Data to Databricks File System (DBFS)

Once the audio processing is complete, move the generated data to the appropriate directories in DBFS.

| File / Data                          | DBFS Destination Path                                             |
|-------------------------------------|-------------------------------------------------------------------|
| `Flattened_Fingerprints.csv` (Songs) | `/dbfs/FileStore/silver/flattened_fingerprints_csv/`              |
| `Flattened_Fingerprints.csv` (Clips) | `/dbfs/FileStore/silver/flattened_fingerprints_clips_csv/`        |

> **Note:** Ensure that these directories exist in DBFS before uploading the files.

---

### 4. Important Note on Notebooks 1 & 2

> **Notebooks 1 and 2 are for demonstration only.**

Due to limitations in Databricks Community Edition, libraries like `librosa` and `yt-dlp` cannot be used reliably in Spark notebooks. Therefore:
- **Notebook 1: Audio Downloader**
- **Notebook 2: Fingerprint Extraction**

These notebooks illustrate how the pipeline would look **if** native audio processing was supported in Databricks. They are **not required or functional** for actual setup and execution.

The main audio processing and fingerprinting should be done **locally** using `AUDIO_PROCESSING.ipynb`.

---

### 5. Continue Workflow in Databricks

Once your data is in DBFS, proceed with the Databricks notebooks as follows:

#### Notebook 3: Fingerprint Hashing
- Loads full song fingerprints from `/silver/flattened_fingerprints_csv`
- Hashes fingerprint pairs using SHA256
- Stores the hashed fingerprints in the **Gold layer**

#### Notebook 4: Clip Hashing & Matching
- Loads clip fingerprints from `/silver/flattened_fingerprints_clips_csv`
- Hashes the clip fingerprints
- Matches clip hashes against the full song hash database
- Computes confidence score

---

### Workflow Summary

```text
[AUDIO_PROCESSING.ipynb (Local)]
     ↓
[Flattened_Fingerprints.csv created for Songs & Clips]
     ↓
[Upload to DBFS:
  - /silver/flattened_fingerprints_csv
  - /silver/flattened_fingerprints_clips_csv]
     ↓
[Notebook 3 (Databricks): Hash full song fingerprints]
     ↓
[Notebook 4 (Databricks): Hash clips → Match → Score → Accuracy Evaluation]
```

---  