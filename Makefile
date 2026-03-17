SHELL := /bin/bash
-include .env
export

SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0
SPARK_LOCAL_CORES ?= 2
SPARK_DRIVER_MEMORY ?= 2g
SPARK_EXECUTOR_MEMORY ?= 2g
SPARK_SHUFFLE_PARTITIONS ?= 2
SPARK_DEFAULT_PARALLELISM ?= 2

.PHONY: setup podman-ready up down logs create-topics dataset-inspect replay bronze-silver bronze-silver-bg bronze-silver-logs bronze-silver-stop gold report-assets reset-stream test format lint

setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

podman-ready:
	@podman info >/dev/null 2>&1 || podman machine start podman-machine-default

up: podman-ready
	podman compose --env-file .env up -d

down: podman-ready
	podman compose --env-file .env down

logs: podman-ready
	podman compose logs -f --tail=100

create-topics: podman-ready
	podman compose --env-file .env up -d kafka
	@echo "Waiting for Kafka to be ready..."
	@for i in $$(seq 1 40); do \
		if podman compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 2; \
	done
	podman compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic $${KAFKA_TOPIC_RAW_GPS:-bus-gps-raw} --partitions 1 --replication-factor 1

dataset-inspect:
	. .venv/bin/activate && python scripts/inspect_dataset.py --data-path $${DATA_RAW_PATH:-./data/raw}

replay:
	. .venv/bin/activate && python -m producer.replay_producer --config configs/pipeline.yaml

bronze-silver: podman-ready
	podman compose --env-file .env up -d kafka spark-master
	@echo "Waiting for Spark master to be ready..."
	@for i in $$(seq 1 40); do \
		if podman compose exec -T -w /opt/project spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --version" >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 2; \
	done
	podman compose exec -T -w /opt/project spark-master /bin/bash -lc "python3 -m pip install --quiet --no-cache-dir --target /tmp/pydeps pyyaml"
	podman compose exec -T -w /opt/project -e PYTHONPATH=/tmp/pydeps:/opt/project -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e KAFKA_STARTING_OFFSETS=$${KAFKA_STARTING_OFFSETS:-earliest} spark-master /opt/spark/bin/spark-submit --master local[$(SPARK_LOCAL_CORES)] --packages $(SPARK_PACKAGES) --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.driver.memory=$(SPARK_DRIVER_MEMORY) --conf spark.executor.memory=$(SPARK_EXECUTOR_MEMORY) --conf spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) --conf spark.default.parallelism=$(SPARK_DEFAULT_PARALLELISM) --conf spark.executorEnv.PYTHONPATH=/tmp/pydeps:/opt/project /opt/project/spark/jobs/bronze_silver_job.py --config /opt/project/configs/pipeline.yaml

bronze-silver-bg:
	mkdir -p logs
	nohup make bronze-silver > logs/bronze-silver.log 2>&1 & echo $$! > logs/bronze-silver.pid
	@echo "Started bronze-silver in background. PID: $$(cat logs/bronze-silver.pid)"

bronze-silver-logs:
	tail -n 120 -f logs/bronze-silver.log

bronze-silver-stop:
	@if [ -f logs/bronze-silver.pid ]; then \
		kill $$(cat logs/bronze-silver.pid) || true; \
		rm -f logs/bronze-silver.pid; \
		echo "Stopped bronze-silver background process."; \
	else \
		echo "No bronze-silver.pid found."; \
	fi

gold: podman-ready
	podman compose --env-file .env up -d spark-master
	@if ! ls data/lakehouse/silver/bus_gps/_delta_log/*.json >/dev/null 2>&1; then \
		echo "Silver Delta table not found. Start bronze-silver, then run replay, then retry gold."; \
		exit 1; \
	fi
	@echo "Waiting for Spark master to be ready..."
	@for i in $$(seq 1 40); do \
		if podman compose exec -T -w /opt/project spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --version" >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 2; \
	done
	podman compose exec -T -w /opt/project spark-master /bin/bash -lc "python3 -m pip install --quiet --no-cache-dir --target /tmp/pydeps pyyaml"
	podman compose exec -T -w /opt/project -e PYTHONPATH=/tmp/pydeps:/opt/project spark-master /opt/spark/bin/spark-submit --master local[$(SPARK_LOCAL_CORES)] --packages $(SPARK_PACKAGES) --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.driver.memory=$(SPARK_DRIVER_MEMORY) --conf spark.executor.memory=$(SPARK_EXECUTOR_MEMORY) --conf spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) --conf spark.default.parallelism=$(SPARK_DEFAULT_PARALLELISM) --conf spark.executorEnv.PYTHONPATH=/tmp/pydeps:/opt/project /opt/project/spark/jobs/gold_job.py --config /opt/project/configs/pipeline.yaml

report-assets:
	. .venv/bin/activate && python scripts/generate_report_assets.py --gold-root ./data/lakehouse/gold --docs-dir ./docs

reset-stream:
	rm -rf data/checkpoints data/lakehouse/bronze data/lakehouse/silver

test:
	. .venv/bin/activate && pytest -q

format:
	. .venv/bin/activate && black producer spark scripts tests

lint:
	. .venv/bin/activate && ruff check producer spark scripts tests
