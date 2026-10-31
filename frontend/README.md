# Event-Driven Notification & Audit Pipeline Dashboard (React)

A standalone, production-ready React 18 + Vite + Tailwind CSS dashboard for monitoring high-throughput, event-driven distributed systems.

Designed to interface with the Spring Boot + Apache Kafka + Redis + PostgreSQL backend.

---

## 🚀 Key Features

- **Live Pipeline Stream (SSE)**: Subscribes to real-time Server-Sent Events (`/api/events/stream`) with status badges (`PROCESSED`, `RATE_LIMITED`, `DUPLICATE_IGNORED`, `DLQ`), event IDs, partition numbers, and latencies.
- **Token Bucket Gauge**: Visualizes token bucket rate limiting (Capacity: 15, Refill: +5/sec) with animated fill gradients and alerts.
- **Event Distribution Chart**: Interactive SVG donut chart with event-type breakdown and hover tooltips.
- **Audit Trail & Filtering**: Full PostgreSQL audit log search by Event ID, type, payload, status dropdown filter, and server-side pagination.
- **Interactive Ingestion Modal**: Submit test events with pre-built JSON templates (Payment, Policy, KYC, Security Alert) and idempotency keys.
- **Traffic Burst Simulation**: Trigger 35 high-velocity concurrent requests to prove Token Bucket throttling (HTTP 429) and Redis SHA-256 deduplication in real time.
- **Configurable Backend Endpoint**: Easily change or test the target API URL at runtime via the settings gear or through `.env`.

---

## 📁 Directory Structure

```text
frontend/
├── Dockerfile                  # Multi-stage production container (Node 20 -> Nginx Alpine)
├── nginx.conf                  # Nginx SPA fallback and gzip compression
├── package.json
├── tailwind.config.js
├── vite.config.js              # Dev proxy routing /api to http://localhost:8080
├── .env.example                # Template for environment variables
├── src/
│   ├── components/
│   │   ├── ArchitectureFlow.jsx # 6-stage workflow visualizer
│   │   ├── ConfigModal.jsx      # Runtime backend API configuration modal
│   │   ├── EventTypeChart.jsx   # Interactive SVG Donut chart
│   │   ├── MetricsCards.jsx     # Ingested, Processed, 429, Deduplicated cards
│   │   ├── PayloadModal.jsx     # Formatted JSON viewer with copy-to-clipboard
│   │   ├── PublishModal.jsx     # Event publishing dialog with presets
│   │   └── TokenBucketGauge.jsx # Token capacity progress visualizer
│   ├── services/
│   │   └── api.js               # Centralized REST & SSE client
│   ├── App.jsx                  # Main dashboard layout and state orchestration
│   ├── main.jsx
│   └── index.css
```

---

## 🛠️ Local Development

### 1. Install Dependencies
```bash
npm install
```

### 2. Configure Backend URL
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
- In development, leaving `VITE_API_BASE_URL` blank will route all `/api/*` calls through Vite's built-in reverse proxy to `http://localhost:8080`.
- To point directly to a running backend:
  ```env
  VITE_API_BASE_URL=http://localhost:8080
  ```

### 3. Run Development Server
```bash
npm run dev
```
Open [http://localhost:5173](http://localhost:5173) in your browser.

---

## 📦 Production Build

```bash
npm run build
```
The optimized static bundle is output to the `dist/` directory.

---

## 🐳 Docker Deployment

### Build the Docker Image
```bash
docker build -t event-pipeline-frontend:latest --build-arg VITE_API_BASE_URL=http://localhost:8080 .
```

### Run the Docker Container
```bash
docker run -d -p 3000:80 --name pipeline-ui event-pipeline-frontend:latest
```
Access the dashboard at [http://localhost:3000](http://localhost:3000).

---

## 🌐 Cloud Deployment Options

### Vercel / Netlify
1. Connect your repository to Vercel or Netlify.
2. Set Root Directory to `frontend`.
3. Set Build Command to `npm run build` and Output Directory to `dist`.
4. Add environment variable:
   - `VITE_API_BASE_URL`: URL of your deployed Spring Boot backend (e.g., `https://api.yourdomain.com`).
