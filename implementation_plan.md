# Host Optivox-v1 For Free

This plan outlines a step-by-step approach to hosting your full-stack application (Optivox-v1) entirely for free. Based on the project structure, it consists of a Vite-based **Frontend** and a Python-based **Backend** (likely FastAPI) that uses local LanceDB storage.

## User Review Required

> [!WARNING]
> **Database Persistence (LanceDB)**
> The free tier of most hosting platforms (like Render or Koyeb) uses **ephemeral (temporary) storage**. Since LanceDB stores its data as files in the `lancedb_data` directory, any new data added to the vector database will be lost every time the free backend goes to sleep or restarts.
> **Workarounds**: 
> 1. If your data is static, you can commit the `lancedb_data` folder to GitHub so it's loaded on startup.
> 2. If you need dynamic persistence for free, you might need to switch to a free cloud vector database like **Pinecone** or **Supabase**, or use **Hugging Face Spaces** (which can offer persistent storage).
>
> Let me know how you want to handle the database storage!

## Open Questions

> [!IMPORTANT]
> 1. **Do you have a GitHub account?** Both free hosting platforms recommended below require your code to be pushed to a GitHub repository.
> 2. **Is your backend using FastAPI?** The startup command depends on your framework. I will assume FastAPI for now (`uvicorn app.main:app`). Let me know if it's different.

## Proposed Hosting Architecture

We will split the deployment into two distinct services using top-tier free platforms:

1. **Frontend (Vite/React/Vue)**: Hosted on **Vercel**. Vercel is the industry standard for frontend hosting, offering an incredibly generous free tier, global CDN, and HTTPS out of the box.
2. **Backend (Python)**: Hosted on **Render** (Web Service Free Tier). Render will automatically build and run your Python backend, exposing an HTTPS endpoint that your frontend can communicate with.

---

### Step 1: Push to GitHub

Before deploying, your code needs to be version-controlled.
1. Create a new public or private repository on [GitHub](https://github.com/).
2. Push your `Optivox-v1` directory to this repository.

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
