FROM node:22-slim

# Install Chromium for Puppeteer + ffmpeg for video processing + fonts
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    ffmpeg \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Puppeteer config: use installed Chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

# Copy package files first for better cache
COPY package*.json ./
RUN npm ci --production

# Copy app files
COPY server.js ./
COPY runnerpro.html ./
COPY moneyball-dashboard.html ./
COPY operational-dashboards.html ./
COPY pipeline.js ./
COPY pipeline_meta.js ./
COPY migrate_to_postgres.js ./
COPY crea_idea.js ./
COPY crea_guion_ceo_media.js ./
COPY crea_visual.js ./
COPY service_account.json ./
COPY estilos/ ./estilos/
COPY prompts/ ./prompts/
COPY creators_md/ ./creators_md/
COPY vertical-sankey/ ./vertical-sankey/

# Create necessary dirs
RUN mkdir -p /tmp/moneyball_pipeline /tmp/moneyball_meta_pipeline

EXPOSE 8080

CMD ["node", "server.js"]
