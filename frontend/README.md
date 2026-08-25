<div align="center">
  <img src="https://via.placeholder.com/150/8a2be2/ffffff?text=OptiVox" alt="OptiVox Logo" width="120" height="120" />

  # OptiVox DB — Frontend UI

  **The React-based Agentic AI SQL Studio Interface**

  [![React](https://img.shields.io/badge/React-19-61dafb?style=for-the-badge&logo=react&logoColor=black)](#)
  [![Vite](https://img.shields.io/badge/Vite-8-646CFF?style=for-the-badge&logo=vite&logoColor=white)](#)

  <p>
    This is the frontend user interface for OptiVox DB. It provides an interactive workspace for AI-powered SQL generation, schema analysis, database tutoring, and CSV-to-SQLite management.
  </p>
</div>

---

## 🛠️ Technology Stack

- **Framework**: React 19, Vite
- **Styling**: Vanilla CSS (Custom Design System with Glassmorphism)
- **Icons**: Lucide React
- **Markdown & Code**: React-Markdown, Remark-GFM
- **HTTP Client**: Axios

*(The backend utilizes FastAPI, LangGraph, Pinecone, and Gemini Embeddings).*

## 🚀 Getting Started

### Prerequisites
- Node.js 18+ and npm/yarn
- The OptiVox backend running locally (see the main repository README for backend setup).

### Installation & Setup

1. **Install Dependencies**
   Navigate to the `frontend` directory and install the necessary npm packages:
   ```bash
   cd frontend
   npm install
   ```

2. **Environment Variables**
   Copy the example environment file and configure it if necessary:
   ```bash
   cp .env.example .env
   ```
   *By default, the Vite dev server expects the backend to be running on `http://127.0.0.1:8000`.*

3. **Run the Development Server**
   Start the Vite development server:
   ```bash
   npm run dev
   ```

4. **Access the Studio**
   Open your browser and visit [http://localhost:5173](http://localhost:5173).

---

## 📂 Structure

```text
frontend/
├── src/
│   ├── components/      # Reusable UI elements
│   ├── contexts/        # React contexts for state management
│   ├── hooks/           # Custom React hooks
│   ├── assets/          # Images, icons, and static assets
│   ├── index.css        # Global CSS and custom design system
│   ├── main.jsx         # Application entry point
│   └── App.jsx          # Root component and routing
├── index.html           # HTML template
├── package.json         # Node dependencies
└── vite.config.js       # Vite configuration
```

## ☁️ Deployment

For production deployment (e.g., to Vercel, Netlify, or Cloudflare Pages):

1. Set the appropriate API base URL in your deployment platform's environment variables:
   ```env
   VITE_API_BASE_URL=https://your-deployed-backend.com/api
   ```
2. Run the build command:
   ```bash
   npm run build
   ```
3. The production-ready files will be generated in the `dist/` directory.

---
<div align="center">
  <p>Built with ❤️ by the OptiVox Team.</p>
</div>
