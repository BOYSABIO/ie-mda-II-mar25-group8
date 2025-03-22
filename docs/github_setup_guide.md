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