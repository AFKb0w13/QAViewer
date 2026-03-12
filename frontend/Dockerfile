FROM node:24-bookworm-slim

WORKDIR /workspace

COPY frontend/package*.json ./frontend/
RUN cd frontend && npm install

COPY frontend ./frontend

WORKDIR /workspace/frontend

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
