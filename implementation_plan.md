# Host Optivox-v1 For Free

This plan outlines a step-by-step approach to hosting your full-stack application (Optivox-v1) entirely for free. Based on the project structure, it consists of a Vite-based **Frontend** and a Python-based **Backend** (likely FastAPI) that uses local LanceDB storage.

## Setup Pinecone (Vector Database)

> [!IMPORTANT]
> Good news! I reviewed the codebase and Optivox-v1 is already built to use **Pinecone** natively for vector storage (in `app/rag/embedder.py`). You do **not** need LanceDB.
> 
> **Action Required**: 
> 1. Go to [Pinecone](https://app.pinecone.io/) and create a free account.
> 2. Create a new Serverless index (name it `optivox-rag`, dimension `768`, metric `cosine`, region `aws us-east-1`).
> 3. Get your API Key. You will need to add `PINECONE_API_KEY` to your environment variables on Render.

## GitHub Status

> [!TIP]
> I have automatically committed your latest changes and pushed them to your GitHub repository `KishoreRam-M/Optivox-v1`. You are ready for deployment!

## Proposed Hosting Architecture

We will split the deployment into two distinct services using top-tier free platforms:

1. **Frontend (Vite/React/Vue)**: Hosted on **Vercel**. Vercel is the industry standard for frontend hosting, offering an incredibly generous free tier, global CDN, and HTTPS out of the box.
2. **Backend (Python)**: Hosted on **Render** (Web Service Free Tier). Render will automatically build and run your Python backend, exposing an HTTPS endpoint that your frontend can communicate with.

---

### Step 1: Push to GitHub (Completed ✅)

Your code is already version-controlled and pushed to your repository (`KishoreRam-M/Optivox-v1`).
You are ready to move on to Step 2!

---

### Step 2: Deploy the Backend to Render (Free)

1. Go to [Render](https://render.com/) and create a free account linked to your GitHub.
2. Click **New +** and select **Web Service**.
3. Connect the GitHub repository you just created.
4. Fill in the following details:
   - **Name**: `optivox-backend`
   - **Language**: `Python`
   - **Branch**: `main`
   - **Root Directory**: Leave blank (or set if your `pyproject.toml` is in a subfolder, but it looks like it's in the root).
   - **Build Command**: Since you use `uv`, you can use `pip install uv && uv sync` or `pip install -r requirements.txt` (if you export it). Let's use: `pip install uv && uv pip install --system .`
   - **Start Command**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT` (Adjust if your app variable or file name is different).
   - **Instance Type**: Select the **Free** tier.
5. Click **Create Web Service**. Wait for the build to finish. Once live, Render will give you a URL (e.g., `https://optivox-backend-xyz.onrender.com`).

---

### Step 3: Connect Frontend to Backend

1. Locally, open your frontend code (e.g., in `.env` or `vite.config.js`).
2. Update the API base URL your frontend uses to make requests from `http://localhost:8000` (or similar) to your new Render URL: `https://optivox-backend-xyz.onrender.com`.
3. Commit and push this change to GitHub.

---

### Step 4: Deploy the Frontend to Vercel (Free)

1. Go to [Vercel](https://vercel.com/) and create a free account linked to your GitHub.
2. Click **Add New...** -> **Project**.
3. Import your `Optivox-v1` GitHub repository.
4. In the configuration settings:
   - **Framework Preset**: Vercel should auto-detect `Vite`.
   - **Root Directory**: Click `Edit` and select the `frontend` folder.
   - **Build Command**: `npm run build`
   - **Output Directory**: `dist`
5. Click **Deploy**. Vercel will build your frontend and provide you with a live URL (e.g., `https://optivox-frontend.vercel.app`).

---

## Verification Plan

### Automated Tests
- N/A for deployment infrastructure setup, but you can configure GitHub actions later.

### Manual Verification
1. Open the Vercel frontend URL in a browser.
2. Verify that the UI loads correctly.
3. Perform an action that triggers a backend API call (e.g., submitting a query).
4. Verify that the backend processes the request and returns the expected result (via the network tab or UI feedback).
