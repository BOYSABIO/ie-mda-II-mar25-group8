## Notebook Breakdown (Databricks Workflow)

### `Notebook 1: Audio Downloader`
- Downloads full songs using `yt_dlp` from YouTube.
- Converts and saves audio files in MP3 format.
- Stores files in: `dbfs:/FileStore/bronze/mp3/`
- Logs basic metadata (`song_name`, `download_status`, `timestamp`) into a Spark DataFrame.
- Outputs logs as CSV or Delta format for traceability and auditing.

---

### `Notebook 2: Fingerprint Extraction`
- Processes the downloaded MP3 files using `librosa`.
- Extracts **spectrogram peaks** and generates **audio fingerprint pairs (constellation maps)**.
- Flattens fingerprints into structured format (e.g., `freq1`, `freq2`, `delta_time`, `anchor_time`).
- Saves flattened fingerprints to: `dbfs:/FileStore/silver/flattened_fingerprints/`

---

### `Notebook 3: Fingerprint Hashing`
- Loads flattened fingerprints from the Silver layer.
- Applies SHA256 hashing using Spark functions (`concat_ws()` + `sha2()`).
- Stores hashed fingerprints (song signature hashes) to the Gold layer at:  
  `dbfs:/FileStore/gold/fingerprint_hashes/`

---

### `Notebook 4: Zasham App – Clip Hashing & Matching`
- Uploads or selects audio clips (e.g., 10–20 sec segments).
- Fingerprints and hashes the clip using the same pipeline as full songs.
- Compares hashed clip fingerprints against the **Gold database** using Spark.
- Performs **matching and confidence scoring**:
  - Joins on `fingerprint_hash`
  - Counts matches per song
  - Calculates confidence score: `(matched_hashes / total_clip_hashes) * 100`
- Ranks top predicted matches and evaluates overall **accuracy of the system**.

---  

### `AUDIO PROCESSING: Full Song & Clip Downloader`
- Combines full song downloading with **automated clip generation**.
- Selects random segments from full MP3 songs to create **short audio clips (e.g., 10–20 seconds)**.
- Clips are saved separately and labeled accordingly (e.g., `clip_songname.mp3`).
- Organizes metadata for both full songs and clips.
- After fingerprinting is completed, this notebook also allows exporting all fingerprint data as a consolidated CSV for downstream use.

---