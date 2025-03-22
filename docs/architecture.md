## Pipeline Architecture

| Stage        | Description                                                                 |
|--------------|------------------------------------------------------------------------------|
| **Bronze**   | Raw MP3 downloads from YouTube                                              |
| **Silver**   | Flattened audio fingerprints (frequency & time bins)                        |
| **Gold**     | Hashed fingerprints for fast lookup and efficient storage                   |
| **Staging**  | Temporary clip fingerprints for matching queries                            |

---