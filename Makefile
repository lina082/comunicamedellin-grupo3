# ComunicaMedellin - Grupo 3 | Modulo 1 ITM 2026
.DEFAULT_GOAL := help
SHELL := /bin/bash
CQLSH := docker exec -i cass1 cqlsh
CH    := docker exec -i clickhouse clickhouse-client --user demo --password demo

.PHONY: help up down clean status schema seed demo-consistencia demo-fanout \
        demo-nodo-caido ch-demo ch-seed up-zk down-zk zk-shell kafka-topics \
        latencia-on latencia-off \
        kafka-consumer kafka-replay api frontend pdf slides logs

help: ## Muestra esta ayuda
	@echo ""
	@echo "  ComunicaMedellin - Grupo 3"
	@echo "  ─────────────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ── Infraestructura ──────────────────────────────────────────
up: ## Levanta el cluster (Cassandra x3 + Kafka KRaft + ClickHouse)
	docker compose up -d
	@echo "Esperando a que Cassandra forme el cluster (~90s)..."
	@until docker exec cass1 nodetool status 2>/dev/null | grep -qE '^UN'; do printf '.'; sleep 5; done
	@echo ""
	@$(MAKE) --no-print-directory status

down: ## Detiene el cluster (conserva datos)
	docker compose down

clean: ## Detiene y BORRA todos los datos
	docker compose down -v
	docker compose -f docker-compose.zookeeper.yml down -v 2>/dev/null || true

status: ## Estado de los 3 nodos Cassandra (para la exposicion)
	@echo "── Cassandra: distribucion de tokens ──"
	@docker exec cass1 nodetool status
	@echo ""
	@echo "── Kafka: topicos ──"
	@docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || true

logs: ## Sigue los logs de todos los servicios
	docker compose logs -f --tail=50

# ── Esquemas y datos ─────────────────────────────────────────
schema: ## Crea el keyspace y las tablas en Cassandra
	$(CQLSH) < infra/cql/schema.cql
	@echo "Esquema Cassandra creado."

seed: ## Carga 10.000 usuarios simulados (requisito del reto)
	python3 demo/scripts/seed_usuarios.py

# ── Demos del reto practico ──────────────────────────────────
latencia-on: ## Simula latencia entre nodos (necesario para ver divergencia)
	@./demo/scripts/latencia_red.sh on

latencia-off: ## Quita la latencia simulada
	@./demo/scripts/latencia_red.sh off

demo-consistencia: ## [RETO] Compara ONE vs QUORUM leyendo de 2 nodos
	@./demo/scripts/latencia_red.sh on
	@echo ""
	python3 demo/scripts/demo_consistencia.py

demo-fanout: ## [RETO] Fan-out de una alerta por particiones/zona
	python3 demo/scripts/demo_fanout.py

demo-nodo-caido: ## [EXTRA] Tumba un nodo y muestra que AP sigue respondiendo
	python3 demo/scripts/demo_nodo_caido.py

# ── ClickHouse (ruta analitica) ──────────────────────────────
ch-seed: ## Genera eventos analiticos en ClickHouse
	python3 demo/scripts/seed_clickhouse.py

ch-demo: ## Ejecuta las consultas OLAP de la exposicion
	@$(CH) --multiquery < infra/clickhouse/consultas-demo.sql

# ── Kafka ────────────────────────────────────────────────────
kafka-topics: ## Detalle de particiones del topico
	docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--describe --topic alertas-ciudadanas

kafka-consumer: ## Consumidor en vivo desde la terminal
	docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic alertas-ciudadanas \
		--property print.partition=true --property print.key=true

kafka-replay: ## Relee desde offset 0 (arquitectura Kappa, Sec. 4.2)
	docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic alertas-ciudadanas \
		--from-beginning --property print.partition=true --timeout-ms 10000

# ── Perfil Zookeeper (aprendizaje) ───────────────────────────
up-zk: ## Levanta Kafka + Zookeeper (perfil de aprendizaje)
	docker compose -f docker-compose.zookeeper.yml up -d
	@echo "Zookeeper en :2181 | Kafka-ZK en :9192"
	@echo "Explorar con: make zk-shell"

down-zk: ## Detiene el perfil Zookeeper
	docker compose -f docker-compose.zookeeper.yml down

zk-shell: ## Shell de Zookeeper: ver brokers, controller, topicos
	@echo "Comandos utiles dentro del shell:"
	@echo "  ls /brokers/ids      -> brokers registrados"
	@echo "  get /controller      -> quien gano la eleccion de lider"
	@echo "  ls /brokers/topics   -> metadata de topicos"
	docker exec -it zookeeper bin/zkCli.sh -server localhost:2181

# ── Aplicacion ───────────────────────────────────────────────
api: ## Arranca la API (FastAPI + WebSocket)
	cd api && python3 -m uvicorn main:app --reload --port 8765

frontend: ## Sirve el frontend en http://localhost:3000
	cd frontend && python3 -m http.server 3000

# ── Documentacion LaTeX ──────────────────────────────────────
pdf: ## Compila el documento principal
	cd docs && pdflatex -interaction=nonstopmode arquitectura.tex >/dev/null && \
		pdflatex -interaction=nonstopmode arquitectura.tex >/dev/null
	@echo "docs/arquitectura.pdf"

slides: ## Compila las diapositivas
	cd docs && pdflatex -interaction=nonstopmode presentacion.tex >/dev/null && \
		pdflatex -interaction=nonstopmode presentacion.tex >/dev/null
	@echo "docs/presentacion.pdf"
