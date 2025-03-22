# Audio Fingerprinting & Matching Pipeline (Shazam-like System)

This project implements a full-scale audio fingerprinting and matching pipeline inspired by Shazam — built using **Apache Spark**, **Databricks**, and **Librosa**. It enables you to process, fingerprint, hash, and match audio clips efficiently in a big data environment.

---

## Project Goals

- Extract audio clips from YouTube using `yt_dlp`
- Convert to MP3, fingerprint using `librosa`, and hash using `SHA256`
- Store data in a structured **Bronze → Silver → Gold** lakehouse architecture
- Match a query clip against a song database and compute **confidence scores**

---  
  
## Documentation

- [Slide Deck](docs/MDA_Group8_Project.pdf)
- [Setup Guide](docs/setup_guide.md)
- [Pipeline Architecture](docs/architecture.md)
- [Notebook Breakdown](docs/notebook_breakdown.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Github Setup Guide](docs/github_setup_guide.md)

---  

## Further Project Ideas

### Lightweight Web App (Frontend UI)
- Simple **Streamlit or Flask app**
- User uploads a clip or types a song name
- Backend runs matching pipeline or calls Databricks via API
- UI displays **best match + confidence score**

---
