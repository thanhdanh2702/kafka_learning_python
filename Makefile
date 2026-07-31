.PHONY: validate up down clean logs topics test tree

validate:
	docker compose --env-file .env config --quiet

up:
	docker compose --env-file .env up -d --build

down:
	docker compose --env-file .env down

clean:
	docker compose --env-file .env down -v

logs:
	docker compose --env-file .env logs -f broker kafbat-ui app

topics:
	docker compose --env-file .env exec broker \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server broker:19092 \
		--list

test:
	python3 -m unittest discover -s tests -v

tree:
	find . -maxdepth 4 -type f | sort
