📘 Documentação — tcc-bridge-api
1. Visão Geral
A tcc-bridge-api é um serviço em Python que conecta dados do Yahoo Finance com sistemas internos. Possui duas funcionalidades principais:

Histórico completo de preços

Cotação atual (último preço)

A ideia é manter o histórico persistido no MongoDB e usar Redis como cache para a cotação em tempo real.

2. Arquitetura
Python — Linguagem principal do serviço.

MongoDB — Armazena dados históricos completos por ticker.

Redis — Cache para última cotação.

Yahoo Finance API — Fonte de dados financeiros.

Scheduler — (ex: cron, APScheduler) para execução periódica (diária/mini-cíclica) das atualizações.

FastAPI / Flask — (opcional) serve endpoints HTTP para consumir os dados.

