FROM node:20-alpine AS builder

WORKDIR /app

# Do NOT set VITE_API_URL or VITE_WS_URL here.
# The nginx reverse proxy handles /api/ and /ws/ routing,
# so the frontend uses relative URLs (empty string).
# For local dev with `npm run dev`, set these in a .env.local file.

COPY package.json ./
RUN npm install

COPY . .
RUN npm run build

# Serve with nginx (supports reverse proxy for API/WS)
FROM nginx:alpine

# Copy built frontend
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx config with reverse proxy rules
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
