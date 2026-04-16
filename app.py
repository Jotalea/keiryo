#!/usr/bin/python3

# ==========================================
# RUN
#
# python -m venv env
# env/bin/pip install fastapi uvicorn python-multipart psutil aiofiles httpx
# env/bin/python app.py
#
# ==========================================

import asyncio
import os
import tempfile
import uuid
import psutil
import re
import httpx
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, StreamingResponse
import aiofiles

# ==========================================
# CONFIGURATION
# ==========================================
CONFIG = {
    # Set to "primary" for the main server handling users.
    # Set to "worker" for headless helper servers.
    "MODE": "primary",

    # How many concurrent ffmpeg tasks this specific node should handle
    # before offloading to workers.
    "MAX_LOCAL_TASKS": 2,

    # List of worker URLs (e.g., ["http://192.168.1.50:8000"]).
    # Ignored if MODE is "worker".
    "WORKERS": []
}
# ==========================================

app = FastAPI(title="keiryo distributed video compressor")
tasks = {}

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>keiryo - video compressor</title>
    <style>
        :root {
            --base: #1e1e2e; --mantle: #181825; --crust: #11111b;
            --text: #cdd6f4; --subtext0: #a6adc8;
            --surface0: #313244; --surface1: #45475a;
            --mauve: #cba6f7; --sapphire: #74c7ec;
            --green: #a6e3a1; --red: #f38ba8; --overlay0: #6c7086;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: var(--base); color: var(--text);
            margin: 0; padding: 20px; display: flex; flex-direction: column; align-items: center;
        }

        .container {
            background-color: var(--mantle); padding: 30px; border-radius: 12px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3); width: 100%; max-width: 600px;
            border: 1px solid var(--surface0);
        }

        h1 { color: var(--mauve); text-align: center; margin-top: 0; }

        .server-status {
            background-color: var(--crust); padding: 10px 15px; border-radius: 8px;
            display: flex; justify-content: space-between; margin-bottom: 25px;
            font-size: 0.9em; border: 1px solid var(--surface0);
        }

        .status-metric { display: flex; align-items: center; gap: 8px; }
        .dot { width: 10px; height: 10px; border-radius: 50%; background-color: var(--green); }

        .form-row { display: flex; gap: 15px; margin-bottom: 15px; }
        .form-group { flex: 1; display: flex; flex-direction: column; gap: 8px; }
        .checkbox-group { display: flex; align-items: center; gap: 10px; margin-bottom: 15px; color: var(--subtext0); font-size: 0.95em;}

        label { font-weight: 600; color: var(--sapphire); font-size: 0.95em; }

        input[type="file"], input[type="number"], select {
            background-color: var(--surface0); color: var(--text); border: 1px solid var(--surface1);
            padding: 10px; border-radius: 6px; font-size: 1rem; outline: none; width: 100%; box-sizing: border-box;
        }

        input[type="checkbox"] { accent-color: var(--mauve); width: 18px; height: 18px; }

        button {
            background-color: var(--mauve); color: var(--crust); border: none;
            padding: 12px; border-radius: 6px; font-size: 1.1rem; font-weight: bold;
            cursor: pointer; transition: opacity 0.2s; width: 100%; margin-top: 10px;
        }
        button:hover { opacity: 0.8; }
        button:disabled { background-color: var(--overlay0); cursor: not-allowed; }

        .progress-container { display: none; margin-top: 25px; text-align: center; }

        .progress-bar-bg {
            background-color: var(--crust); border-radius: 8px; width: 100%; height: 24px;
            margin-top: 10px; overflow: hidden; border: 1px solid var(--surface0);
        }
        .progress-bar-fill {
            background-color: var(--green); height: 100%; width: 0%;
            transition: width 0.3s; display: flex; align-items: center; 
            justify-content: center; color: var(--crust); font-weight: bold; font-size: 0.85em;
        }

        .log-box {
            background-color: var(--crust); color: var(--subtext0); padding: 15px;
            border-radius: 6px; font-family: monospace; font-size: 0.85em;
            margin-top: 15px; height: 80px; overflow-y: auto; text-align: left;
        }

        .download-btn {
            display: none; background-color: var(--green); margin-top: 15px; text-align: center;
            text-decoration: none; color: var(--crust); padding: 12px; border-radius: 6px; font-weight: bold;
        }
    </style>
</head>
<body>

    <div class="container">
        <h1>keiryo 軽量</h1>

        <div class="server-status">
            <div class="status-metric"><div class="dot" id="cpu-dot"></div> cpu: <span id="cpu-val">--%</span></div>
            <div class="status-metric"><div class="dot" id="ram-dot"></div> ram: <span id="ram-val">--%</span></div>
            <div class="status-metric"><span id="mode-val" style="color: var(--mauve); font-weight: bold;"></span></div>
        </div>

        <form id="uploadForm">
            <div class="form-group" style="margin-bottom: 15px;">
                <label for="video">select video file</label>
                <input type="file" id="video" name="video" accept="video/*" required>
            </div>

            <div class="form-row">
                <div class="form-group">
                    <label for="target_mb">target (MB)</label>
                    <input type="number" id="target_mb" name="target_mb" value="8" min="1" max="100" required>
                </div>
                <div class="form-group">
                    <label for="preset">preset</label>
                    <select id="preset" name="preset">
                        <option value="fast">fast</option>
                        <option value="medium" selected>medium</option>
                        <option value="slow">slow</option>
                    </select>
                </div>
            </div>

            <div class="form-row">
                <div class="form-group">
                    <label for="skip_first">skip start (sec)</label>
                    <input type="number" id="skip_first" value="0" min="0" step="0.1">
                </div>
                <div class="form-group">
                    <label for="skip_last">skip end (sec)</label>
                    <input type="number" id="skip_last" value="0" min="0" step="0.1">
                </div>
            </div>

            <div class="checkbox-group">
                <input type="checkbox" id="remove_audio"> <label for="remove_audio">remove audio</label>
            </div>
            <div class="checkbox-group">
                <input type="checkbox" id="auto_download" checked> <label for="auto_download">auto-download</label>
            </div>
            <div class="checkbox-group">
                <input type="checkbox" id="play_sound" checked> <label for="play_sound">play sound on finish</label>
            </div>

            <button type="submit" id="submitBtn">compress video</button>
        </form>

        <div class="progress-container" id="progressContainer">
            <h3 style="color: var(--sapphire)" id="statusText">uploading...</h3>

            <div class="progress-bar-bg">
                <div class="progress-bar-fill" id="progressBar">0%</div>
            </div>

            <div class="log-box" id="logBox">waiting to start...</div>
            <a href="#" class="download-btn" id="downloadBtn" download>download video</a>
        </div>
    </div>

    <script>
        function playAlertSound() {
            try {
                const ctx = new (window.AudioContext || window.webkitAudioContext)();
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.type = 'sine';
                osc.frequency.setValueAtTime(880, ctx.currentTime); // A5 note
                gain.gain.setValueAtTime(0.1, ctx.currentTime);
                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.start();
                osc.stop(ctx.currentTime + 0.3);
            } catch(e) { console.log("audio not supported"); }
        }

        setInterval(async () => {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                document.getElementById('cpu-val').innerText = `${data.cpu}%`;
                document.getElementById('ram-val').innerText = `${data.ram}%`;
                document.getElementById('mode-val').innerText = data.mode.toUpperCase();

                document.getElementById('cpu-dot').style.backgroundColor = data.cpu > 85 ? 'var(--red)' : 'var(--green)';
                document.getElementById('ram-dot').style.backgroundColor = data.ram > 85 ? 'var(--red)' : 'var(--green)';
            } catch (e) {}
        }, 3000);

        document.getElementById('uploadForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const fileInput = document.getElementById('video');
            if (fileInput.files.length === 0) return;

            const submitBtn = document.getElementById('submitBtn');
            const progressContainer = document.getElementById('progressContainer');
            const statusText = document.getElementById('statusText');
            const logBox = document.getElementById('logBox');
            const downloadBtn = document.getElementById('downloadBtn');
            const progressBar = document.getElementById('progressBar');

            const autoDownload = document.getElementById('auto_download').checked;
            const playSound = document.getElementById('play_sound').checked;

            submitBtn.disabled = true;
            progressContainer.style.display = 'block';
            downloadBtn.style.display = 'none';
            statusText.innerText = "uploading...";
            progressBar.style.width = '0%';
            progressBar.innerText = '0%';
            logBox.innerText = "sending file...";

            const formData = new FormData();
            formData.append('video', fileInput.files[0]);
            formData.append('target_mb', document.getElementById('target_mb').value);
            formData.append('preset', document.getElementById('preset').value);
            formData.append('remove_audio', document.getElementById('remove_audio').checked);
            formData.append('skip_first', document.getElementById('skip_first').value);
            formData.append('skip_last', document.getElementById('skip_last').value);

            try {
                const response = await fetch('/api/compress', { method: 'POST', body: formData });
                const data = await response.json();
                if (data.task_id) pollTask(data.task_id);
                else throw new Error("Failed to start task");
            } catch (err) {
                statusText.innerText = "Error!"; statusText.style.color = "var(--red)";
                logBox.innerText = err.message; submitBtn.disabled = false;
            }

            function pollTask(taskId) {
                const interval = setInterval(async () => {
                    const res = await fetch(`/api/task/${taskId}`);
                    const taskData = await res.json();

                    statusText.innerText = taskData.status === 'processing' ? 'processing...' :
                                           taskData.status === 'completed' ? 'success!' : 'error!';

                    const progress = taskData.progress || 0;
                    progressBar.style.width = `${progress}%`;
                    progressBar.innerText = `${progress}%`;

                    logBox.innerText = taskData.message + "\\n" + (taskData.error || "");
                    logBox.scrollTop = logBox.scrollHeight;

                    if (taskData.status === 'completed') {
                        clearInterval(interval);
                        downloadBtn.href = `/api/download/${taskId}`;
                        downloadBtn.style.display = 'block';
                        submitBtn.disabled = false;

                        if (playSound) playAlertSound();
                        if (autoDownload) downloadBtn.click();

                    } else if (taskData.status === 'error') {
                        clearInterval(interval);
                        statusText.style.color = "var(--red)";
                        submitBtn.disabled = false;
                        progressBar.style.backgroundColor = "var(--red)";
                    }
                }, 2000);
            }
        });
    </script>
</body>
</html>
"""

@app.get("/")
async def get_ui():
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/api/status")
async def get_server_status():
    return {
        "cpu": psutil.cpu_percent(interval=None),
        "ram": psutil.virtual_memory().percent,
        "mode": CONFIG["MODE"]
    }

async def run_ffmpeg_pass(cmd: str, task_id: str, pass_num: int, total_duration: float):
    """Executes FFmpeg and parses stderr line-by-line to calculate progress %"""
    # Default to 33% if the regex fails to catch anything
    tasks[task_id]["progress"] = 33

    process = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    async def read_stream():
        while True:
            line = await process.stderr.readline()
            if not line: break
            line_str = line.decode('utf-8', errors='ignore')

            # Look for "time=00:00:15.50" to calculate progress
            match = re.search(r"time=(\d{2}):(\d{2}):(\d{2}\.\d{2})", line_str)
            if match and total_duration > 0:
                h, m, s = match.groups()
                current_sec = int(h) * 3600 + int(m) * 60 + float(s)

                # Pass 1 is 0-50%, Pass 2 is 50-100%
                perc = (current_sec / total_duration) * 50
                base_perc = 0 if pass_num == 1 else 50
                tasks[task_id]["progress"] = min(100, int(base_perc + perc))

    await asyncio.gather(read_stream(), process.wait())
    return process.returncode

async def process_video(task_id: str, input_path: str, target_mb: float, preset: str, remove_audio: bool, skip_first: float, skip_last: float):
    work_dir = os.path.dirname(input_path)
    output_path = os.path.join(work_dir, f"{task_id}_output.mp4")
    log_prefix = os.path.join(work_dir, f"passlog_{task_id}")

    try:
        tasks[task_id]["message"] = "Extracting video duration..."

        # 1. Get Duration
        cmd_dur = f"ffprobe -v error -select_streams v:0 -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 \"{input_path}\""
        proc = await asyncio.create_subprocess_shell(cmd_dur, stdout=asyncio.subprocess.PIPE)
        out, _ = await proc.communicate()

        if proc.returncode != 0 or not out: raise Exception("Could not read video duration.")
        original_duration = float(out.decode().strip())
        if original_duration <= 0: raise Exception("Duration is 0.")

        # 2. Trim math calculation
        valid_skip_first = max(0.0, skip_first)
        valid_skip_last = max(0.0, skip_last)

        if valid_skip_first + valid_skip_last >= original_duration:
            raise Exception("Skip times are equal to or exceed total video duration.")

        target_duration = original_duration - valid_skip_first - valid_skip_last
        trim_cmd = ""
        if valid_skip_first > 0: trim_cmd += f" -ss {valid_skip_first}"
        if target_duration < original_duration: trim_cmd += f" -t {target_duration}"

        # 3. Bitrate calculation (using the target duration, not original)
        total_kbits = target_mb * 8192
        total_kbps = int(total_kbits / target_duration)
        audio_kbps = 0 if remove_audio else 64
        video_kbps = max(50, total_kbps - audio_kbps)

        audio_cmd = "-an" if remove_audio else f"-c:a aac -b:a {audio_kbps}k"

        # 4. Pass 1
        tasks[task_id]["message"] = f"Pass 1/2: Analyzing... (Bitrate: {video_kbps}kbps)"
        cmd_pass1 = (
            f"ffmpeg -y {trim_cmd} -i \"{input_path}\" -c:v libx265 -preset {preset} "
            f"-b:v {video_kbps}k -maxrate {video_kbps}k -bufsize {video_kbps*2}k "
            f"-fpsmax 30 -pix_fmt yuv420p -pass 1 -passlogfile \"{log_prefix}\" -an -f null /dev/null"
        )
        rc1 = await run_ffmpeg_pass(cmd_pass1, task_id, pass_num=1, total_duration=target_duration)
        if rc1 != 0: raise Exception("Pass 1 failed. Check encoding parameters.")

        # 5. Pass 2
        tasks[task_id]["message"] = "Pass 2/2: Encoding video..."
        cmd_pass2 = (
            f"ffmpeg -y {trim_cmd} -i \"{input_path}\" -c:v libx265 -preset {preset} "
            f"-b:v {video_kbps}k -maxrate {video_kbps}k -bufsize {video_kbps*2}k "
            f"-fpsmax 30 -pix_fmt yuv420p -pass 2 -passlogfile \"{log_prefix}\" "
            f"{audio_cmd} \"{output_path}\""
        )
        rc2 = await run_ffmpeg_pass(cmd_pass2, task_id, pass_num=2, total_duration=target_duration)
        if rc2 != 0: raise Exception("Pass 2 failed.")

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["message"] = "Processing complete!"
        tasks[task_id]["progress"] = 100
        tasks[task_id]["output_path"] = output_path

    except Exception as e:
        tasks[task_id]["status"] = "error"
        tasks[task_id]["error"] = str(e)
    finally:
        try: os.remove(input_path)
        except: pass
        try: os.remove(f"{log_prefix}-0.log")
        except: pass

async def offload_to_worker(task_id: str, input_path: str, filename: str, target_mb: float, preset: str, remove_audio: bool, skip_first: float, skip_last: float, worker_url: str):
    """Background task to push file to a worker node via HTTP"""
    try:
        async with httpx.AsyncClient(timeout=None) as client:
            with open(input_path, 'rb') as f:
                files = {'video': (filename, f, 'video/mp4')}
                data = {
                    'target_mb': target_mb, 'preset': preset,
                    'remove_audio': str(remove_audio).lower(),
                    'skip_first': skip_first, 'skip_last': skip_last
                }
                res = await client.post(f"{worker_url}/api/compress", files=files, data=data)
                res.raise_for_status()
                # Link local task ID to worker's task ID
                tasks[task_id]["worker_task_id"] = res.json()["task_id"]
    except Exception as e:
        tasks[task_id] = {"status": "error", "error": f"Failed to offload to {worker_url}: {e}", "is_remote": True, "progress": 0}
    finally:
        # We don't need the local file anymore once sent to the worker
        try: os.remove(input_path)
        except: pass

@app.post("/api/compress")
async def start_compression(
    background_tasks: BackgroundTasks,
    video: UploadFile = File(...),
    target_mb: float = Form(8.0),
    preset: str = Form("medium"),
    remove_audio: bool = Form(False),
    skip_first: float = Form(0.0),
    skip_last: float = Form(0.0)
):
    task_id = str(uuid.uuid4())
    temp_dir = tempfile.gettempdir()

    ext = os.path.splitext(video.filename)[1] or ".mp4"
    input_path = os.path.join(temp_dir, f"{task_id}_input{ext}")

    async with aiofiles.open(input_path, 'wb') as out_file:
        content = await video.read()
        await out_file.write(content)

    # --- CLUSTER LOAD BALANCING LOGIC ---
    local_active = len([t for t in tasks.values() if t.get("status") == "processing" and not t.get("is_remote")])

    if CONFIG["MODE"] == "primary" and local_active >= CONFIG["MAX_LOCAL_TASKS"] and CONFIG["WORKERS"]:
        import random
        worker_url = random.choice(CONFIG["WORKERS"]).rstrip('/')

        tasks[task_id] = {
            "status": "processing", "message": f"Offloading task to worker...",
            "progress": 0, "is_remote": True, "worker_url": worker_url, "worker_task_id": None
        }
        background_tasks.add_task(offload_to_worker, task_id, input_path, video.filename, target_mb, preset, remove_audio, skip_first, skip_last, worker_url)
        return {"task_id": task_id}

    # Process Locally
    tasks[task_id] = {
        "status": "processing", "message": "Queued locally...",
        "progress": 0, "is_remote": False
    }
    background_tasks.add_task(process_video, task_id, input_path, target_mb, preset, remove_audio, skip_first, skip_last)
    return {"task_id": task_id}

@app.get("/api/task/{task_id}")
async def get_task_status(task_id: str):
    if task_id not in tasks:
        return JSONResponse({"error": "Task not found"}, status_code=404)

    t = tasks[task_id]

    # Proxy status request to worker if offloaded
    if t.get("is_remote") and t.get("worker_task_id"):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(f"{t['worker_url']}/api/task/{t['worker_task_id']}")
                return res.json()
        except:
            return {"status": "processing", "message": "Syncing with worker...", "progress": t.get("progress", 33)}

    return t

@app.get("/api/download/{task_id}")
async def download_file(task_id: str):
    if task_id not in tasks:
        return JSONResponse({"error": "Task missing"}, status_code=404)

    t = tasks[task_id]

    # Proxy streaming the file back from the worker so CORS/IPs don't break for the client
    if t.get("is_remote") and t.get("worker_task_id"):
        async def stream_remote_file():
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", f"{t['worker_url']}/api/download/{t['worker_task_id']}") as response:
                    async for chunk in response.aiter_bytes():
                        yield chunk
        return StreamingResponse(
            stream_remote_file(),
            media_type="video/mp4",
            headers={"Content-Disposition": f"attachment; filename=compressed_{task_id}.mp4"}
        )

    # Serve Locally
    if t["status"] != "completed":
        return JSONResponse({"error": "File not ready"}, status_code=404)

    output_path = t["output_path"]
    if not os.path.exists(output_path):
         return JSONResponse({"error": "File deleted or missing"}, status_code=404)

    return FileResponse(
        path=output_path,
        filename=f"keiryo_{os.path.basename(output_path)}",
        media_type="video/mp4"
    )

if __name__ == "__main__":
    import uvicorn
    psutil.cpu_percent(interval=0.1)
    print(f"Starting {CONFIG['MODE'].upper()} Node on http://0.0.0.0:8620")
    uvicorn.run(app, host="0.0.0.0", port=8620)
