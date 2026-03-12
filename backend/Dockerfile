FROM node:24-bookworm-slim

WORKDIR /workspace

COPY backend/package*.json ./backend/
RUN cd backend && npm install

COPY backend ./backend
COPY data/generated ./data/generated

WORKDIR /workspace/backend

CMD ["npm", "run", "dev"]
