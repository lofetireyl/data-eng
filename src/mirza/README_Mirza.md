# Starten der Kafka, Spark und PostgreSQL Umgebung

## Umgebungsvariablen

export KAFKA_BOOTSTRAP=localhost:9092 \
export SPOTIFY_CLIENT_SECRET=<spotify_client_secret>\
export SPOTIFY_CLIENT_ID=<spotify_client_id>\
export POLL_SEC=<kafka_producer_intervall_zum_spotify_API_pollen>\
export MARKETS=<Länder> # Bsp.: AT,DE,CH für DACH 

# Starten der Conntainer

## Zuerst Kafka, Zoekeeper und PostgreSQL starten
```
docker compose up -d kafka zookeeper postgres
```

Nach dem Starten kann man sich mit 

```
docker ps
```

die Container anzeigen lassen:


### Beispiel
```
❯ docker ps
CONTAINER ID   IMAGE                             COMMAND                  CREATED             STATUS             PORTS                                                                                      NAMES
73a0e8f33c53   postgres:16                       "docker-entrypoint.s…"   About an hour ago   Up About an hour   0.0.0.0:5432->5432/tcp, :::5432->5432/tcp                                                  postgres
26fb6470c0c8   confluentinc/cp-kafka:7.6.1       "/etc/confluent/dock…"   About an hour ago   Up About an hour   0.0.0.0:9092->9092/tcp, :::9092->9092/tcp, 0.0.0.0:29092->29092/tcp, :::29092->29092/tcp   group_project_spotify-kafka-1
74dbdd0689b0   confluentinc/cp-zookeeper:7.6.1   "/etc/confluent/dock…"   About an hour ago   Up About an hour   2181/tcp, 2888/tcp, 3888/tcp                                                               group_project_spotify-zookeeper-1
```

## Erstellen der Topics am Kafka Broker 

```
export KAFKA_CID=<kafka_container_id_ vom_docker_ps_befehl_vorher>
```

## Erstellen der Topics am Kafka Broker
```
docker exec -it "$KAFKA_CID" bash -lc '
  kafka-topics --bootstrap-server kafka:29092 --create --topic spotify.new_releases.album --partitions 3 --replication-factor 1 || true
  kafka-topics --bootstrap-server kafka:29092 --create --topic spotify.new_releases.track --partitions 3 --replication-factor 1 || true
  kafka-topics --bootstrap-server kafka:29092 --create --topic spotify.audio_features     --partitions 3 --replication-factor 1 || true
  kafka-topics --bootstrap-server kafka:29092 --create --topic spotify.artist_meta        --partitions 3 --replication-factor 1 || true

  echo "Topics now:"
  kafka-topics --bootstrap-server kafka:29092 --list
'
```

## Checken ob die Topics erstellt wurden

```
❯ docker exec -it $KAFKA_CID bash -lc 'kafka-topics --bootstrap-server kafka:29092 --list'
```

## Check ob Messages am Kafka Broker sind

```
❯ docker exec -it $KAFKA_CID bash -lc \
  'kafka-console-consumer --bootstrap-server kafka:29092 --topic spotify.new_releases.album --from-beginning --max-messages 3'
```

## Starten des Kafka Brokers von der Konsole aus
```
python producer/kafka_producer_new_releases.py
```

> Warten bis der Kafka Producer die ersten Messages empfangen hat.

## Starten des Spark Containers
```
docker compose up -d spark
```

Nach einer Weile sollte Spark die ersten Einträge von Kafka in die PostgreSQL Datenbank gestreamt haben.

Mit

```
❯ docker compose logs -f spark
```

kann man sich die Logs vom Spark Container anschauen.

## Testen ob Daten in der PostgreSQL Datenbank sind

````
❯ docker exec -it postgres psql -U spotify -d spotify -c "SELECT COUNT(*) albums FROM spotify_album;"
 albums
--------
     50
(1 row)
````

und 

```
❯ docker exec -it postgres psql -U spotify -d spotify -c \
"SELECT album_id, album_name, market, fetched_at FROM spotify_album ORDER BY fetched_at DESC LIMIT 10;"
        album_id        |             album_name             | market |       fetched_at
------------------------+------------------------------------+--------+------------------------
 1ovzOV64ES8l2X7t1iIhEO | Death Above Life                   | AT     | 2025-10-04 13:43:49+00
 1cqqIH16XzBnDchrXJ9Enf | Vie                                | AT     | 2025-10-04 13:43:49+00
 5ckEQxkxWVwC6SMPu0z266 | The Beauty Of It All               | AT     | 2025-10-04 13:43:49+00
 4Q1Rf1xZ5OiQcCFnSAHdeE | THAT'S SHOWBIZ BABY!               | AT     | 2025-10-04 13:43:49+00
 0xRRqJIhWIU0WASStddsWK | 21 Gramm                           | AT     | 2025-10-04 13:43:49+00
 3Xk1Mz1aaon4cx1f91Z00n | Solace & The Vices                 | AT     | 2025-10-04 13:43:49+00
 5gi65SySOtiSeEmMMvr2xf | Ich lieb mich, ich lieb mich nicht | AT     | 2025-10-04 13:43:49+00
 1LsNb6mVitbLZCxnRsbCDJ | AM I THE DRAMA?                    | AT     | 2025-10-04 13:43:49+00
 2iQTfdkR1rEOBr5p7tp7SO | Back to the Boots                  | AT     | 2025-10-04 13:43:49+00
 4a6NzYL1YHRUgx9e3YZI6I | The Life of a Showgirl             | AT     | 2025-10-04 13:43:49+00
(10 rows)
```

### Starten der gesamten Umgebung

```
docker compose up
```

> Eventuell ist das Builden des Kafka Producer Containers, also der Script was new releases zieht, notwendig

```
❯ docker compose up -d --build producer
```

Logs beobachten

```
❯ docker compose logs -f spark\
❯ docker compose logs -f producer
```
