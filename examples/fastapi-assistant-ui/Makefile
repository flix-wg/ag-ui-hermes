ROOT := ../..
HERMES_PORT ?= 8642
SERVER_PORT ?= 8000
UV_RUN := uv run --project $(ROOT) --extra fastapi --with uvicorn==0.54.0

.PHONY: install fake-hermes server web demo

install:  ## build the local js package and install the web app
	cd $(ROOT)/js && npm ci && npm run build
	cd web && npm ci

fake-hermes:  ## the test suite's fake Hermes on HERMES_PORT, key demo-key
	PORT=$(HERMES_PORT) $(UV_RUN) python server/fake_hermes_server.py

server:  ## the FastAPI app on SERVER_PORT; set HERMES_URL and HERMES_API_KEY
	$(UV_RUN) uvicorn --app-dir server app:app --port $(SERVER_PORT)

web:  ## the Vite dev server on 5173, proxying /agent to SERVER_PORT
	cd web && SERVER_PORT=$(SERVER_PORT) npm run dev

demo:  ## all three, against the fake Hermes; Ctrl-C stops everything
	trap 'kill 0' EXIT; \
	PORT=$(HERMES_PORT) $(UV_RUN) python server/fake_hermes_server.py & \
	HERMES_URL=http://127.0.0.1:$(HERMES_PORT) HERMES_API_KEY=demo-key \
		$(UV_RUN) uvicorn --app-dir server app:app --port $(SERVER_PORT) & \
	cd web && SERVER_PORT=$(SERVER_PORT) npm run dev
